package com.huginn.DB.backend.VersionManager;

import com.huginn.DB.backend.DataManager.DataManager;
import com.huginn.DB.backend.TransactionManager.TransactionManager;
import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;
import com.huginn.DB.backend.common.AbstractCache;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;

import javax.transaction.xa.Xid;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Version Manager的实现类
 * Entry的缓存
 */

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;                             //需要使用到事务功能
    DataManager dm;                                    //需要使用到dm功能
    Map<Long,Transaction> activeTransactions;          //维护一个活跃的事务表
    Lock lock;
    LockTable lt;                                      //维护一个有向图，用于检测死锁

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransactions = new HashMap<>();
        //加入0号事务，总是提交
        activeTransactions.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID,0,null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /**
     * 提供给上层的接口
     */

    /**
     * 读取一个entry返回其data部分，注意可见性
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        //获取xid事务，用于判断可见性
        Transaction t = activeTransactions.get(xid);
        lock.unlock();

        if (t.err != null){
            throw t.err;
        }

        Entry entry = null;
        try {
            //获取entry
            entry = super.get(uid);
        } catch (Exception e) {
            //如果不存在该entry
            if (e == Error.NullEntryException){
                return null;
            }else {
                throw e;
            }
        }

        try {
            //判断可见性
            if (Visibility.isVisible(tm,t,entry)){
                //可见
                return entry.data();
            }else {
                return null;
            }
        } finally {
            entry.release();
        }

    }

    /**
     * 插入数据
     * 先封装成entry再封装成DataItem交由dm插入
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        //获取共享资源需要加锁
        Transaction t = activeTransactions.get(xid);
        lock.unlock();

        if (t.err != null){
            throw t.err;
        }

        //生成entry格式的byte[] [XMIN] [XMAX] [data]
        byte[] raw = Entry.wrapEntryRaw(xid,data);
        //将raw封装成DataItem并插入
        return dm.insert(xid,raw);
    }

    /**
     * 删除一个entry
     * 1. 是可见性判断
     * 2. 获取资源的锁
     * 3. 版本跳跃判断
     * 4. 删除的操作只有一个设置 XMAX
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        //获取共享资源需要加锁
        Transaction t = activeTransactions.get(xid);
        lock.unlock();

        if (t.err != null){
            throw t.err;
        }

        //删除前需要先获取entry
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            //没找到该entry
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }

        try {
            //判断是否可见
            if (!Visibility.isVisible(tm,t,entry)){
                //不可见无法删除
                return false;
            }

            //该记录对于该事务可见
            Lock l = null;
            try {
                //拿到锁
                l = lt.add(xid,uid);
            } catch (Exception e) {
                //出现死锁
                t.err = Error.ConcurrentUpdateException;
                //自动abort
                internAbort(xid,true);
                t.autoAborted = true;
                throw t.err;
            }

            if (l != null){
                //阻塞等待
                l.lock();
                l.unlock();
            }

            //已经被本事务删除了
            if (entry.getXmax() == xid){
                return false;
            }

            //出现版本跳跃，自动回滚
            if (Visibility.isVersionSkip(tm,t,entry)){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid,true);
                t.autoAborted = true;
                throw t.err;
            }

            //删除entry
            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构,将其存放在 activeTransaction 中，用于检查和快照使用
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            //开启一个事务，并初始化Transaction结构
            long xid = tm.begin();
            //用于快照
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransactions);
            activeTransactions.put(xid,transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态
     * 这里调用事务的方法，同时还要处理transaction这个事务的抽象结构(事务隔离)和LockTable(检测死锁)
     */
    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransactions.get(xid);
        lock.unlock();
        try{
            if (t.err != null)
                throw t.err;
        }catch (NullPointerException e){
            System.out.println(xid);
            System.out.println(activeTransactions.keySet());
            Panic.panic(e);
        }

        lock.lock();
        activeTransactions.remove(xid);          //从活跃名单中移除该xid
        lock.unlock();
        lt.remove(xid);                          //从图中将其删除
        tm.commit(xid);
    }

    /**
     *  手动回滚
     *  abort 事务的方法则有两种，手动和自动。
     *  手动指的是调用 abort() 方法，
     *  而自动，则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚
     */
    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    /**
     * abort事务
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransactions.get(xid);
        if (!autoAborted){
            activeTransactions.remove(xid);
        }
        lock.unlock();

        if (t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    /**
     * 从磁盘获取一个entry
     * 1. 读取DataItem
     * 2. 封装成entry
     */
    @Override
    protected Entry getForceCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    /**
     * 从缓存中释放掉一个entry
     */
    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    /**
     * 释放entry
     */
    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }
}
