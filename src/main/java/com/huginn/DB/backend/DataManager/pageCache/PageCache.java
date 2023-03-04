package com.huginn.DB.backend.DataManager.pageCache;

import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存
 */

public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13;  //页面

    int newPage(byte[] initData);

    Page getPage(int pgno) throws Exception;

    void close();

    void release(Page page);

    void truncateByPgno(int maxPgno);

    int getPageNumber();

    void flushPage(Page pg);


    //创建一个PageCache, 对一个db文件的页面进行缓存
    public static PageCacheImpl create(String path, long memory){
        //创建一个db文件
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
           Panic.panic(e);
        }

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
        //给该文件创建一个页面缓存，其最大资源数＝最大页面数
        return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }

    //打开一个db文件，返回其对应的PageCache
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);

        if (!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }

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
        return new PageCacheImpl(raf,fc,(int)memory/PAGE_SIZE);
    }
}
