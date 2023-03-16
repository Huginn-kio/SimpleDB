package com.huginn.DB.backend.server;

import com.huginn.DB.backend.TableManager.TableManager;
import com.huginn.DB.transport.Encoder;
import com.huginn.DB.transport.Package;
import com.huginn.DB.transport.Packager;
import com.huginn.DB.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.*;

public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;           //监听的端口
        this.tbm = tbm;
    }

    /**
     * Server启动
     * 启动一个 ServerSocket 监听端口，当有请求到来时直接把请求丢给一个新线程处理
     */
    public void start(){
        ServerSocket ss = null;
        try {
            ss= new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("HugDB Server listen to port:" + port + ".....");

        //corePoolSize: 核心线程数最大值
        //maximumPoolSize: 拥有的最多线程数
        //keepAliveTime: 空闲线程的存活时间  当当前线程数 > corePoolSize, 此时线程的空闲时间达到这个时间则被停掉(将线程数收缩回corePoolSize)
        //workQueue: 缓存任务的阻塞队列(排队策略)
        //rejectedExecutionHandler: 拒绝新任务的策略

        //当调用线程池execute() 方法添加一个任务时，线程池会做如下判断:
        //如果有空闲线程，则直接执行该任务；
        //如果没有空闲线程，且当前运行的线程数少于corePoolSize，则创建新的线程执行该任务；
        //如果没有空闲线程，且当前的线程数等于corePoolSize，同时阻塞队列未满，则将任务入队列，而不添加新的线程；
        //如果没有空闲线程，且阻塞队列已满，同时池中的线程数小于maximumPoolSize ，则创建新的线程执行任务；
        //如果没有空闲线程，且阻塞队列已满，同时池中的线程数等于maximumPoolSize ，则根据构造函数中的 handler 指定的策略来拒绝新的任务。

        //ArrayBlockingQueue: 一个由数组支持的有界阻塞队列。此队列按 FIFO（先进先出）原则对元素进行排序。一旦创建了这样的缓存区，就不能再增加其容量。
        // 试图向已满队列中放入元素会导致操作受阻塞；试图从空队列中提取元素将导致类似阻塞。
        //CallerRunsPolicy: 由向线程池提交任务的线程来执行该任务
        ThreadPoolExecutor thdPool = new ThreadPoolExecutor(10, 20, 1L,
                TimeUnit.SECONDS,new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

        try {
            while(true){
                Socket socket = ss.accept();  //开始监听port端口
                Runnable worker = new HandleSocket(socket, tbm);  //将请求调给一个新线程处理
                thdPool.execute(worker);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                ss.close();
            } catch (IOException e){
            }
        }
    }
}

//处理请求的线程
class HandleSocket implements Runnable{
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }

    //处理请求
    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();   //得到客户端的信息
        System.out.println("Establish Connection: " + address.getAddress().getHostName() + ":" + address.getPort());

        Packager packager = null;

        try {                                                  //封装一个用于收发的Packager
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t,e);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return;
        }

        //执行器，负责执行sql语句返回结果，需要绑定一个tbm
        Executor exe = new Executor(tbm);

        //不断地读取, 如果接收/发送package时出现异常则中断循环, 关闭
        while(true){
            Package pkg = null;
            try {
                pkg = packager.receive();    //读取一个package
            } catch (Exception e) {
                break;
            }
            byte[] sql = pkg.getData();      //得到package中的数据部分，也就是SQL语句
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception ex) {
                e = ex;                             //当执行sql语句发生错误时，则返回错误信息, 否则返回查询结果
                e.printStackTrace();
            }

            pkg = new Package(result,e);      //将查询结果和异常打包成一个package

            try {
                packager.send(pkg);
            } catch (IOException ex) {
                ex.printStackTrace();
                break;
            }
        }

        exe.close();

        try {
            packager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
