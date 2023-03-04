package com.huginn.DB.backend.DataManager.logger;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志
 * HugDB提供了崩溃后的数据恢复功能。DM 层在每次对底层数据操作时，都会记录一条日志到磁盘上。在数据库奔溃之后，再次启动时，可以根据日志的内容，恢复数据文件，保证其一致性。
 * 日志的二进制文件，按照如下的格式进行排布：[XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 * XChecksum: 一个四字节的整数，是对后续所有日志计算的校验和
 * Log1 ~ LogN: 常规的日志数据
 * BadTail: 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在
 * 每条日志的格式: [Size][Checksum][Data]
 * Size: 是一个四字节整数，标识了 Data 段的字节数
 * Checksum: 该条日志的校验和
 */

public class LoggerImpl implements Logger {

    private static final int SEED = 13331;   //生成校验和的种子
    private static final int OF_SIZE = 0;    //每条日志的格式: [Size][Checksum][Data], 每条日志的size的偏移
    private static final int OF_CHECKSUM = OF_SIZE + 4;   //每条日志的checksum的偏移
    private static final int OF_DATA = OF_CHECKSUM + 4;  //每条日志的data的偏移
    public static final String LOG_SUFFIX = ".log";
    private RandomAccessFile file;                       //表示该日志文件
    private FileChannel fc;                              //文件的channel
    private Lock lock;

    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xChecksum) {
        this.file = file;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }


    private long position;  // 当前日志指针的位置,当前日志文件读到的位置偏移
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;  // 当前日志的总校验和


    /**
     * 初始化
     * 获取总校验和
     * 赋值log文件大小
     * 校验日志文件并删除badtail
     */
    void init(){
        //读取日志文件的文件大小
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }

        //4字节为校验和的大小，为文件的最小长度
        if (size < 4){
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);

        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;

        checkAndRemoveTail();
    }

    //单条日志的校验和，其实就是通过一个指定的种子实现的，根据seed和log字节数组生成校验和
    private int calChecksum(int xCheck, byte[] log){
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * next() 方法的实现主要依靠 internNext()，其中 position 是当前日志文件读到的位置偏移：
     * 返回的是日志的data部分
     */
    @Override
    public byte[] next() {
        lock.lock();
        //读一条日志
        try {
            byte[] log = internNext();
            if (log == null) return null;
            //返回日志的data部分
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从position处读一条日志
     */
    private byte[] internNext(){
        //position为当前日志文件的偏移，即文件中某条日志开头，如果这个位置 + size + checksum >= filesize, 说明这是一个badtail
        if (position + OF_DATA >= fileSize){
            return null;
        }

        //读取4字节的size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }

        int size = Parser.parseInt(tmp.array());

        //当前位置 + 8 字节信息 + data 超出了文件范围，即这是一个badtail
        if (position + OF_DATA + size > fileSize){
            return null;
        }

        //读取整个log数据，提取出checksum和data
        ByteBuffer buf =  ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        //log
        byte[] log = buf.array();

        //获取checksum
        int checksum1 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));

        //获取data调用calChecksum(xcheck, log)计算checksum
        int checksum2 = calChecksum(0,Arrays.copyOfRange(log,OF_DATA,log.length));

        //进行校验，校验成功则说明log没问题返回log，如果检验失败返回null
        if (checksum2 == checksum1){
            position += log.length;
            return log;
        }else {
            return null;
        }
    }

    /**
     * 在打开一个日志文件时，需要首先校验日志文件的 XChecksum，并移除文件尾部可能存在的 BadTail
     * 由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和
     * 去掉 BadTail 即可保证日志文件的一致性。
     */
    private void checkAndRemoveTail(){
        //初始化position位置为所有日志的开头
        rewind();

        //求所有日志的校验和(不包括BadTail)
        int xCheck = 0;
        while (true){
            //校验和检验的是所有日志，不单单是其data部分，所以使用internNext
            byte[] log = internNext();
            //读到文件末尾或者BadTail时退出
            if (log == null) break;
            xCheck = calChecksum(xCheck,log);
        }

        //检验校验和是否一致
        if (xCheck != xChecksum){
            //检验不一致，则说明日志文件出现问题
            Panic.panic(Error.BadLogFileException);
        }

        try {
            //position位置为最后一条日志的末尾，后面可能是空或者是BadTail，需要把BadTail截掉
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //重新将position设置为所有日志的开头位置，方便后续的提取日志
        rewind();
    }


    @Override
    public void rewind() {
        this.position = 4;
    }

    /**
     * 截掉x之后的文件内容，保留x之前的文件内容
     */
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 写入日志
     * 向日志文件写入日志时，也是首先将数据包裹成日志格式，写入文件后，再更新文件的校验和，更新校验和时，会刷新缓冲区，保证内容写入磁盘。
     */
    @Override
    public void log(byte[] data) {
        //将数据包装成日志格式
        byte[] log = wrapLog(data);
        //导入buffer
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            //fc.size()返回这个Channel连接的文件大小,将position定位到文件末尾
            fc.position(fc.size());
            //写入文件
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }

        //更新文件的总的校验和
        updateXCheckSum(log);
    }

    /**
     * 在加入日志log后更新总的checksum
     */
    private void updateXCheckSum(byte[] log) {
        //在原有的xchecksum上计算新的checksum
         this.xChecksum = calChecksum(this.xChecksum,log);

         //导入buffer
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(this.xChecksum));

        try {
            //写入新的checksum
            fc.position(0);
            fc.write(buf);
            //调用 force() 方法强制将数据刷出到磁盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    /**
     * data -> log
     * + checksum
     * + size
     * 返回log格式的日志
     */
    private byte[] wrapLog(byte[] data) {
        //计算该日志的检验和
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        //计算该日志的size
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size,checksum,data);
    }


    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }



}
