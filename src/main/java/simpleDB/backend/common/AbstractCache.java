package simpleDB.backend.common;

import simpleDB.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    private Map<Long, Node> cache;                     // 实际缓存的数据
    private Map<Long, Boolean> getting;             // 资源是否正在源被获取
    private Node lruHead, lruTail;
    private int maxResource;                            // 缓存的最大缓存资源数
    private Lock lock;


    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new ConcurrentHashMap<>();
        getting = new ConcurrentHashMap<>();
        lruHead = new Node();
        lruTail = new Node();
        lruHead.next = lruTail;
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true) {
            if (getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                Node node = cache.get(key);
                T obj = node.val;
                lock.lock();
                moveToHead(node);
                lock.unlock();
                return obj;
            }

            lock.lock();
            // 尝试获取该资源
            if (maxResource > 0 && cache.size() == maxResource) {
                //淘汰一个Page
                Node node = lruTail.pre;
                removeNode(node);
                cache.remove(node.key);
                releaseForCache(node.val);
            }

            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            getting.remove(key);
            throw e;
        }

        lock.lock();
        Node node = new Node(key, obj);
        getting.remove(key);
        cache.put(key, node);
        addToHead(node);
        lock.unlock();
        return obj;
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key).val;
                releaseForCache(obj);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);

    /**
     * 维护LRU链表的操作
     */

    private void moveToHead(Node node) {
        removeNode(node);
        addToHead(node);
    }

    private void removeNode(Node node) {
        node.pre.next = node.next;
        node.next.pre = node.pre;
        node.next = null;
        node.pre = null;
    }

    private void addToHead(Node node) {
        node.next = lruHead.next;
        lruHead.next.pre = node;
        node.pre = lruHead;
        lruHead.next = node;
    }

    private class Node {
        Long key;
        T val;
        Node pre;
        Node next;

        public Node() {
        }

        public Node(Long key, T val) {
            this.key = key;
            this.val = val;
        }
    }
}
