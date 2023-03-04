package com.huginn.DB.backend.common;

import com.huginn.DB.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */

public  abstract  class AbstractCache<T> {

    //缓存
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 资源的引用个数
    private HashMap<Long, Boolean> getting;             // 正在被获取的资源，为了应对多线程场景,需要记录哪些资源正在从数据源获取中（从数据源获取资源是一个相对费时的操作）

    //缓存参数
    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;


    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    //从缓存中获取关键字为key的资源
    protected T get(long key) throws Exception{

        //进入死循环，无限尝试从缓存中获取
        while(true){
            lock.lock();
            //请求的数据正在被其他线程从数据源中获取
            if (getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            //其他线程获取完毕，判断数据是否在缓存中
            if(cache.containsKey(key)){
                //数据在缓存中，增加一个对该资源的引用并直接返回
                T obj = cache.get(key);
                references.put(key,references.get(key)+1);
                lock.unlock();
                return obj;
            }

            //数据不在缓存中，需要去数据源获取数据
            //先判断缓存是否已经满了
            if (maxResource > 0 && count == maxResource){
                //缓存已经满了
                lock.unlock();
                throw Error.CacheFullException;
            }

            //缓存未满，即将去数据源获取数据
            count++; //缓存数据 + 1
            getting.put(key,true); //设置正在从数据源获取该数据
            lock.unlock();
            break;
        }

        //去数据源获取数据
        T obj = null;
        try {
            //去数据源获取数据
            obj = getForceCache(key);
        } catch (Exception e) {
            //获取失败
            lock.lock();
            count--;  //count是共享数据，需要加锁进行操作
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        //成功将数据从数据源获取进缓存区
        lock.lock();
        getting.remove(key);
        cache.put(key,obj);
        references.put(key,1);
        lock.unlock();

        return obj;

    }

    //从缓存中释放关键字为key的资源
    protected void release(long key){
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            //已经没有线程在使用这个资源
            if (ref == 0){
                T obj = cache.get(key);
                //将资源写回到数据源
                releaseForCache(obj);
                //从缓存中删除该资源
                references.remove(key);
                cache.remove(key);
                count--;
            }else {
                references.put(key,ref);
            }
        } finally {
            lock.unlock();
        }
    }

    //关闭缓存，写回所有资源
    protected void close(){
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                release(key);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }



    //当资源不在缓存时的获取行为
    protected abstract T getForceCache(long key) throws Exception;

    //当资源被驱逐时的写回行为
    protected abstract void releaseForCache(T obj);
}
