package com.huginn.DB.backend.DataManager.dataItem;

import com.huginn.DB.backend.DataManager.DataManagerImpl;
import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.common.SubArray;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 * DataItem: [ValidFlag] [DataSize] [Data]
 * ValidFlag: 标识了该 DataItem 是否有效,  1字节 (删除一个 DataItem，只需要简单地将其有效位设置为 0)
 * DataSize:  标识了后面 Data 的长度, 2 字节
 * DataItem不单单是一种提供给上层的抽象的数据，实际上一个DataItem在磁盘上也是如此的结构组织的
 */

public class DataItemImpl implements DataItem {
    static final int OF_VALID = 0;         //validFlag的偏移
    static final int OF_SIZE = 1;          //DataSize的偏移
    static final int OF_DATA = 3;          //data的偏移

    private SubArray raw;                  //记录了DataItem: [ValidFlag] [DataSize] [Data]
    private byte[] oldRaw;                 //记录修改前的前相
    private DataManagerImpl dm;            //实现了dataItem的缓存管理
    private Lock rLock;                    //读锁
    private Lock wLock;                    //写锁
    private long uid;                      //该DataItem的唯一标识，表示其在磁盘中的存储位置，页数(4) + (2) + 页内偏移(2)
    private Page pg;                       //该DataItem的数据所在的页

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 返回dataItem的data(共享数组)
     */
    @Override
    public SubArray data() {
        //返回DataItem的data部分
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 判断该DataItem是否有效
     * 1:无效
     * 0:有效
     */
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }


    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
     * 在修改之前需要调用 before() 方法，
     * 想要撤销修改时，调用 unBefore() 方法，
     * 在修改完成后，调用 after() 方法。
     * 整个流程，主要是为了保存前相数据，并及时落日志。
     * DM 会保证对 DataItem 的修改是原子性的。
     */

    /**
     * 修改前的准备
     * 写前加锁
     * 设置其对应的页为脏页
     * 保存前相数据
     */
    @Override
    public void before() {
       //修改数据，加锁
        wLock.lock();
        //对DataItem的修改即是对其对应的页进行修改，将该页设置为脏页
        pg.setDirty(true);
        //将前相数据保存在oldRaw中
        System.arraycopy(raw.raw, raw.start, oldRaw,0,oldRaw.length);
    }

    /**
     * 撤销修改，即修改了进行撤销
     * 进行数据的恢复
     * 释放锁
     */
    @Override
    public void unBefore() {
        System.arraycopy( oldRaw,0,raw.raw, raw.start,oldRaw.length);
        wLock.unlock();
    }

    /**
     * 修改完成的步骤
     * 落日志 —— 修改型日志
     * 释放锁
     */
    @Override
    public void after(long xid) {
        //将修改落日志
        dm.logDataItem(xid,this);
        wLock.unlock();
    }

    /**
     * 在使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem）。
     * DM是DataItem的缓存
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
