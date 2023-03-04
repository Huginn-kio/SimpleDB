package com.huginn.DB.backend.DataManager.logger;

import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 日志接口
 */

public interface Logger {
    void log(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();


    /**
     * 创建日志Logger
     */

    public static Logger create(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);

        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //初始化文件中的Xchecksum为0
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));

        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    /**
     * 打开日志Logger
     */

    public static Logger open(String path) {
        File f = new File(path + LoggerImpl.LOG_SUFFIX);

        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }

}
