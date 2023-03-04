package com.huginn.DB.backend.DataManager.pageCache;

import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.DataManager.page.PageImpl;
import com.huginn.DB.backend.common.AbstractCache;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 以页为单位的缓存结构
 */


public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;      //页面缓存中最小的页面数

    public static final String DB_SUFFIX = ".db";    //db文件管理

    private RandomAccessFile file;                   //从文件系统读取的db文件，这个页面缓存对应着这个文件

    private FileChannel fc;                         //NIO读取文件中的channel

    private Lock fileLock;

    private AtomicInteger pageNumbers;              //记录当前打开的file中有多少页


    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        //maxResource定义缓存中页面的可能的最大数量，当其值少于规定的最小的页面限制数量时，报错
        if (maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            //文件的长度
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();

        //file里共有多少页面
        this.pageNumbers = new AtomicInteger((int)length/PAGE_SIZE);

    }

    //pgno在文件中对应的偏移
    private static long pageOffset(int pgno){
        return (pgno - 1) * PAGE_SIZE;
    }

    //当资源不在缓存时的获取行为 —— 从file中读取对应的数据封装成page
    @Override
    protected Page getForceCache(long key) throws Exception {
        //key为页号
        int pgno = (int)key;
        //得到该页在文件中的偏移
        long offset = PageCacheImpl.pageOffset(pgno);
        //在buffer开辟一个8kb的区域
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();

        //读取文件
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        fileLock.unlock();
        //将读取的数据封装成Page
        return new PageImpl(pgno,buf.array(),this);
    }

    //当资源被驱逐时的写回行为 —— 判断页面是否是脏页，来决定是否需要写回文件系统
    @Override
    protected void releaseForCache(Page page) {
        //脏页
        if (page.isDirty()){
          flush(page);
          page.setDirty(false);
        }
    }

    //页面写回
    public void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = PageCacheImpl.pageOffset(pgno);

        fileLock.lock();
        //将page的实际存放的数据放入到NIO中的buffer
        ByteBuffer buf = ByteBuffer.wrap(pg.getData());

        try {
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }

    }


    //页面缓存中新建一页，需要立即写回到文件系统中
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.getAndIncrement();
        Page page = new PageImpl(pgno,initData,null);
        flush(page);
        return pgno;
    }


    //从缓存中获取pgno对应的页面
    @Override
    public Page getPage(int pgno) throws Exception {
        //调用了AbstractCache中从缓存中获取资源的方法
        return get((long)pgno);
    }


    //关闭缓存
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    @Override
    public void release(Page page) {
        release((long)page.getPageNumber());
    }


    //通过maxPgno将文件进行截断，截断后的文件的最大页数为maxPgno
    @Override
    public void truncateByPgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);

        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }

        pageNumbers.set(maxPgno);
    }

    /**
     * 返回文件的最大页数
     */
    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }


    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }


}
