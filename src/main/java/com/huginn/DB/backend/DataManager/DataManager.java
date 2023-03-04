package com.huginn.DB.backend.DataManager;

import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.DataManager.logger.Logger;
import com.huginn.DB.backend.DataManager.page.PageOne;
import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.DataManager.pageCache.PageCacheImpl;
import com.huginn.DB.backend.TransactionManager.TransactionManager;

public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    /**
     * 从已有文件创建 DataManager 和从空文件创建 DataManager 的流程稍有不同
     * 除了 PageCache 和 Logger 的创建方式有所不同以外
     * 从空文件创建首先需要对第一页进行初始化
     * 而从已有文件创建，则是需要对第一页进行校验，来判断是否需要执行恢复流程。并重新对第一页生成随机字节。
     */

    /**
     * 从空文件创建 DataManager
     * 1. 创建PageCache
     * 2. 创建日志Logger
     * 3. 初始化第一页 —— 在缓存中生成一页并进行验证码设置，并落盘
     */
    public static DataManager create(String path, long memory, TransactionManager tm) {
        PageCacheImpl pc = PageCache.create(path, memory);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, tm, lg);
        dm.initPageOne();
        return dm;
    }

    /**
     * 从特定路径打开 DataManager
     * 1. 打开相应的PageCache
     * 2. 打开相应的日志Logger
     * 3. 对第一页进行校验
     * 4. 校验失败则恢复，校验成功则继续执行
     * 5. 初始化PageIndex
     * 6. 重新生成验证码
     * 7. 第一页落盘
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCacheImpl pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, tm,lg);

        //对第一页的验证码进行校验
        if (!dm.loadCheckPageOne()){
            //第一页出错，进行校验
            Recover.recover(tm,lg,pc);
        }

        //因为打开的是已经存在的数据库文件，里面有数据，所以需要进行PageIndex的初始化
        dm.fillPageIndex();
        //重新设置第一页的验证码
        PageOne.setVcOpen(dm.pageOne);
        //将第一页的修改(验证码)落盘
        dm.pc.flushPage(dm.pageOne);
        return dm;
    }

}
