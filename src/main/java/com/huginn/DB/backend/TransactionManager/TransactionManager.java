package com.huginn.DB.backend.TransactionManager;

import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin();                       // 开启一个新事务
    void commit(long xid);              // 提交一个事务
    void abort(long xid);               // 取消一个事务
    boolean isActive(long xid);         // 查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);      // 查询一个事务的状态是否是已提交
    boolean isAborted(long xid);        // 查询一个事务的状态是否是已取消
    void close();                       // 关闭TM


    //创建事务管理，及其管理的xid文件
    public static TransactionManagerImpl create(String path){
        //打开file
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);

        //在该路径下创建一个文件，如果该路径已经存在文件则返回false
        try {
            if (!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        //文件不能读写
        if (!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //创建8字节头部初始值为0
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);

        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }

    public static TransactionManagerImpl open(String path){
        //打开file
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);

        //文件不存在
        if (!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }

        //文件不能读写
        if (!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf,fc);
    }

}

