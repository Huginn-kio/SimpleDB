package com.huginn.DB.backend.TransactionManager;

import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TransactionManager负责管理事务状态以及存放事务状态的xid文件 —— 对应该文件和对文件的IO
 */

public class TransactionManagerImpl implements TransactionManager {
    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务占用长度
    private static final int XID_FIELD_SIZE = 1;
    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;
    //超级事务的XID为0，其事务状态永远为committed
    private static final long SUPER_XID = 0;
    //规定XID文件后缀
    static final String XID_SUFFIX = ".xid";
    //XID文件, RandomAccessFile可以自由访问文件的任意位置
    private RandomAccessFile file;
    //在NIO中提供新的方式对文件进行I/O操作，非阻塞方式进行IO
    private FileChannel fc;
    //记录当前的记录数
    private long xidCounter;
    //锁
    private Lock counterLock;


    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        //ReentrantLock(可重入锁) 支持公平锁和非公平锁，
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }


    //开始一个事务，返回XID
    @Override
    public long begin() {
        //加锁
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            //更新xid事务的状态为ACTIVE
            updateXID(xid,FIELD_TRAN_ACTIVE);
            //XIDCounter + 1，并修改xid文件header
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }


    //提交事务
    @Override
    public void commit(long xid) {
        updateXID(xid,FIELD_TRAN_COMMITTED);
    }

    //取消事务，进行回滚
    @Override
    public void abort(long xid) {
        updateXID(xid,FIELD_TRAN_ABORTED);
    }

    //判断事务的状态
    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID || xid > this.xidCounter) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID || xid > this.xidCounter) return false;
        return checkXIDStatus(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID || xid > this.xidCounter) return false;
        return checkXIDStatus(xid,FIELD_TRAN_ABORTED);
    }


    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //检查XID文件是否合法
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        //当文件长度少于头部时，明显出错
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        //nio中thread通过channel和buffer进行交流
        //用于分配新的字节缓冲区
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        try {
            //从第0字节，读取信息到buffer中
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        //获取LEN_XID_HEADER_LENGTH 8字节所代表的记录数
        this.xidCounter = Parser.parseLong(buf.array());
        //定位到最后一条记录的后面
        long end = getXidPosition(this.xidCounter + 1);
        //如果最后一条记录的结尾 ！= 文件结尾，则文件不符合xid
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }

    //到达第xid个记录的开头处
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    //更新xid的状态
    private void updateXID(long xid, byte status) {
        //xid记录的开头
        long offset = getXidPosition(xid);
        //字节数组存放status,长度是一个字节
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        //将字节数组放入缓冲区
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try{
            //文件定位到该记录的开头，并将status写入该处
            fc.position(offset);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    //给XIDCounter+1,且修改header
    private void incrXIDCounter() {
        xidCounter++;
        //将当前记录数转成byte[]，然后放入缓冲区
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        try {
            //header位于文件的首8个字节
            fc.position(0);
            fc.write(buf);
            /**
             * 为了减少访问磁盘的次数，通过文件通道对文件进行操作之后可能不会立即刷出到磁盘，此时如果系统崩溃，将导致数据的丢失。
             * 为了减少这种风险，在进行了重要数据的操作之后应该调用 force() 方法强制将数据刷出到磁盘。
             * 无论是否对文件进行过修改操作，即使文件通道是以只读模式打开的，只要调用了 force(metaData) 方法，就会进行一次 I/O 操作。
             * 参数 metaData 指定是否将元数据（例如：访问时间）也刷出到磁盘。
             */
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    //检查xid的事务是否处于status状态
    private boolean checkXIDStatus(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return buf.array()[0] == status;
    }

}
