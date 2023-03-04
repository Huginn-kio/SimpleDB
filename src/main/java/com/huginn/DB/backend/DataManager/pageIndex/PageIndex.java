package com.huginn.DB.backend.DataManager.pageIndex;

import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.DataManager.pageCache.PageCacheImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面索引：缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
 * 本质上，将所有页的空闲空间进行了区间划分，页的空闲空间为 0 ~ 8kb，对应着区间 0 ~ INTERVALS_NO, 对应的空闲空间对应落在相应的空间
 * 这里将页的闲置空间信息封装成一个PageInfo，其中维持着一个list数组，数组从 1 - 40，每个元素对应着一个list，装载落在该区间的页号码
 * 记录所有页的free space，用于在插入数据时分配空间
 */

public class PageIndex {

    //将所有页根据free space划分成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCacheImpl.PAGE_SIZE / INTERVALS_NO;  //每个区间的大小
    private List<PageInfo>[] lists;    //维护了INTERVALS_NO个list, 每个list存放着多个pageInfo, 记录着对应的页及其空闲空间
    private Lock lock;

    public PageIndex() {
        this.lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<PageInfo>();
        }
    }

    /**
     * 根据所需要的空间来到对应的区间挑选一个页面来进行数据的插入
     */
    public PageInfo select(int spaceSize){
        lock.lock();
        //对所需的空间的区间向上取整
        int pgno = (int) Math.ceil(spaceSize/THRESHOLD);
        try {
            while (pgno <= INTERVALS_NO){
                //从>=所需要空间的区间上寻找页面并返回，挑选的页面若存在则一定满足其需求
                if (lists[pgno].size() == 0){
                    pgno++;
                    continue;
                }
                //返回的页会从PageIndex中移除，即同一个页面是不允许并发写的
                // 在上层模块使用完这个页面后，需要将其重新插入PageIndex
                return lists[pgno].remove(0);
            }
            //无满足要求的页面
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 将页面根据闲置空间放入对应的区间
     */
    public void add(int pgno, int freeSpace){
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }




}
