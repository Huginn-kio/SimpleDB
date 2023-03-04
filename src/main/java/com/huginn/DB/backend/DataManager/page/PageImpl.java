package com.huginn.DB.backend.DataManager.page;

import com.huginn.DB.backend.DataManager.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * dm从文件系统读取资源到内存中的基本单位，页(位于内存，PageCache)
 */

public class PageImpl implements Page {
    private int pageNumber;      //该页面的页号，从1开始
    private byte[] data;         //该页面实际包含的字节数据
    private boolean dirty;       //标志该页面是否是脏页面，在缓存驱逐的时候，脏页需要被写回磁盘
    private Lock lock;
    private PageCache pc;        //用于在拿到该页的引用时可以快速对该页面的缓存进行释放


    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        this.lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {
        pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return this.dirty;
    }

    @Override
    public int getPageNumber() {
        return this.pageNumber;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }
}
