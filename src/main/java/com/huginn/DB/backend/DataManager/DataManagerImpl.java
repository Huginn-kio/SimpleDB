package com.huginn.DB.backend.DataManager;

import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.DataManager.dataItem.DataItemImpl;
import com.huginn.DB.backend.DataManager.logger.Logger;
import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.DataManager.page.PageOne;
import com.huginn.DB.backend.DataManager.page.PageX;
import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.DataManager.pageIndex.PageIndex;
import com.huginn.DB.backend.DataManager.pageIndex.PageInfo;
import com.huginn.DB.backend.TransactionManager.TransactionManager;
import com.huginn.DB.backend.common.AbstractCache;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.Types;
import com.huginn.DB.common.Error;

/**
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 */

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    PageCache pc;                               //一个DataItem对应着Page里的一段数据，页号+页内偏移
    PageIndex pIndex;                           //dm管理着pageIndex，在dm启动时，进行pageIndex的填充
    TransactionManager tm;                      //dm提供事务功能
    Logger logger;
    Page pageOne;

    public DataManagerImpl(PageCache pc, TransactionManager tm, Logger logger) {
        super(0);
        this.pc = pc;
        this.pIndex = new PageIndex();
        this.tm = tm;
        this.logger = logger;
    }

    /**
     * DM 层提供了三个功能供上层使用，分别是读、插入和修改。
     * 修改是通过读出的 DataItem 实现的，于是 DataManager 只需要提供 read() 和 insert() 方法。
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        //如果该dataItem失效
        if (!dataItem.isValid()){
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    /**
     * 插入数据，返回该dataItem的UID
     * 1. 获得一个足以放入的页面
     * 2. 写日志
     * 3. 插入数据
     * 4. 将页面重新插入pageIndex
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        //获得DataItem格式的数据 [ValidFlag] [DataSize] [Data]
        byte[] raw = DataItem.wrapDataItemRaw(data);
        //数据太大放不进一页
        if(raw.length > PageX.MAX_FREE_SPACE){
            throw Error.DataTooLargeException;
        }

        //尝试获取一个合适的页面
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            //获取该页号
            pg = pc.getPage(pi.pgno);
            //生成插入日志信息
            byte[] log = Recover.insertLog(xid, pg, data);
            //写入日志
            logger.log(log);
            //插入数据，返回该数据在文件的起始位置
            short offset = PageX.insert(pg, data);
            pg.release();
            return Types.addressToUid(pi.pgno,offset);
        } finally {
            //将取出的page重新插回到pIndex
            if (pg != null){
                pIndex.add(pi.pgno,PageX.getFreeSpace(pg));
            }else {
                pIndex.add(pi.pgno,freeSpace);
            }
        }
    }

    /**
     * 正常关闭时，需要执行缓存和日志的关闭流程，不要忘了设置第一页的字节校验
     */
    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    /**
     * 根据uid从磁盘中读取对应位置的数据封装成DataItem
     *
     * @param uid 8bytes，pgno (4) + (2) + offset (2)
     */
    @Override
    protected DataItem getForceCache(long uid) throws Exception {
        //分别取出页号和页内偏移
        short offset = (short) (uid & ((1L << 16) - 1)); //取出最后2字节的数据，offset
        uid >>>= 32;
        int pgno = (int) (uid & (1L << 32) - 1); //取出前面4字节的数据
        //获取到page，如果该页不在内存中则会从磁盘中读入
        Page page = pc.getPage(pgno);
        return DataItem.parseDataItem(page, offset, this);
    }

    /**
     * 释放DataItem，需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，只需要将 DataItem 所在的页 release 即可
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /**
     * 在DM被创建时，需要获取所有页面，并充填PageIndex，形成一个记录着各页free space的list
     */
    public void fillPageIndex() {
        //得到文件中的页总数
        int pageNumber = pc.getPageNumber();
        //第一页存储的是一些启动用的数据，第二页才开始存储数据
        for (int i = 2; i <= pageNumber; i++) {
            //遍历page
            Page page = null;
            try {
                //会将对应的页读至缓存
                page = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            //将其封装成PageInfo
            pIndex.add(page.getPageNumber(), PageX.getFreeSpace(page));
            //将该页从缓存中释放，防止缓存中存储过多的页
            page.release();
        }
    }

    /**
     * 初始化第一页
     */
    public void initPageOne() {
        //在缓存中创建一页并返回页数
        //HugDB 的第一页，只是用来做启动检查。具体的原理是，在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。
        int pgno = pc.newPage(PageOne.InitRaw());
        //判断是否只有一页
        assert pgno == 1;
        try {
            //获取该页
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        //将第一页落盘
        pc.flushPage(pageOne);
    }

    /**
     * 检验第一页, 因为打开dm时还没有对pageOne进行赋值，所以要先从磁盘上获取PageOne
     */
    public boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 释放掉DataItem
     */
    public void releaseDataItem(DataItemImpl di) {
            super.release(di.getUid());
    }

    /**
     * 生成修改型日志并落盘
     */
    public void logDataItem(long xid, DataItemImpl di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }
}
