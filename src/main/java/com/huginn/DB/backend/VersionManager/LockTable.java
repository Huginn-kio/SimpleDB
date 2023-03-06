package com.huginn.DB.backend.VersionManager;

import com.huginn.DB.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 用一个有向图来检测是否存在死锁 —— 含有环
 */

public class LockTable {
    private Map<Long, List<Long>> x2u;          // 某个XID已经获得的资源的UID列表   xid: list(uid)
    private Map<Long, Long> u2x;                // UID被某个XID持有              uid: xid
    private Map<Long, List<Long>> wait;         // 正在等待UID的XID列表           uid: list(xid)
    private Map<Long, Lock> waitLock;           // 正在等待资源的XID的锁           xid: lock
    private Map<Long, Long> waitU;              // XID正在等待的UID              xid: uid
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 这里是出现等待才进行加边和返回锁
     * 出现等待情况，有向图就增加一条边，并进行死锁检测
     * 如果检测到死锁，则撤销这条边，不允许添加，并撤销该事务
     * 不需要等待则返回null，否则返回锁对象
     * 会造成死锁则抛出异常
     * 调用add，如果需要等待的话，会返回一个上了锁的 Lock 对象。
     * 调用方(别的线程)在获取到该对象时，需要尝试获取该对象的锁，由此实现阻塞线程的目的
     */
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try{
            if (isInList(x2u,xid,uid)){
                // xid已经拿到了uid资源，不需要等待
                return null;
            }

            //xid还未持有uid，且uid目前没有xid持有，无需等待
            if (!u2x.containsKey(uid)){
                //xid持有uid，修改u2x和x2u
                u2x.put(uid,xid);
                putInList(x2u,xid,uid);
                return null;
            }

            //xid未持有uid，且uid目前已经被其他xid持有
            //为xid的等待资源列表中加入uid，在等待uid的列表中加入xid
            waitU.put(xid,uid);
            putInList(wait,uid,xid);
            //判断是否有死锁
            if (hasDeadLock()){
                //存在死锁
                //撤销分配
                waitU.remove(xid);
                removeFromList(wait,uid,xid);
                throw Error.DeadlockException;
            }

            //不存在死锁，在waitLock中加入xid和lock,并返回一个上了锁的lock对象
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid,l);
            return l;
        }finally {
            lock.unlock();
        }
    }


    /**
     * 通过有向图是否存在环来判断是否存在死锁
     * 原理：对每一个xid以其为首进行一次dfs(因为不一定是连通图)，stamp记录第几次dfs，xidStamp记录是否存在环
     * t1 -> t2 -> t1 此时xidStamp有(t1, 1) (t2,1) 当对t2进行dfs时，会检查xidStamp中是否存在t1,且stamp=1(说明是同一次dfs)这样来检测是否存在环
     */
    private Map<Long,Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        //对每个xid为首进行一次dfs
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            //说明xid已经在之前的dfs中进行了找环且不存在环，即xid在之前的别的xid为首的dfs找环中处于路径的节点上，且无环，所以不需要以它来首进行找环了因为不存在环
            if (s != null && s > 0){
                continue;
            }
            stamp++;

            if (dfs(xid)){
                //dfs中存在环，则返回true
                return true;
            }
        }
        return false;
    }

    /**
     * 对xid进行dfs进行找环
     */
    private boolean dfs(long xid) {
        //查看该xid是否遍历过
        Integer s = xidStamp.get(xid);
        //该xid已遍历过，且是本次dfs的之前的节点，即已经出现环
        if (s != null && s == stamp){
            return true;
        }
        //该xid已遍历过，且不是本次dfs的节点，说明该节点已经进行过找环且无环，所以不需要再找环
        if (s != null && s < stamp){
            return false;
        }
        //该xid没有遍历过，加入本次dfs
        xidStamp.put(xid,stamp);

        //查看这个xid的等待资源
        Long uid = waitU.get(xid);
        //此xid没有请求等待资源
        if (uid == null) return false;
        //查看此资源的拥有者
        Long uidOwner = u2x.get(uid);
        //因为此uid资源有xid在等待，所以这个uid一定存在使用者
        assert uidOwner != null;
        //去查看此资源拥有者
        return dfs(uidOwner);
    }

    /**
     * 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
     */
    public void remove(long xid){
        lock.lock();
        try {
            //释放掉xid所拥有的所有资源
            List<Long> resources = x2u.get(xid);
            if (resources != null && !resources.isEmpty()){
                while (resources.size() > 0){
                    long uid = resources.remove(0);
                    //释放掉该资源，并使等待该资源的事务进行占用
                    selectNewXID(uid);
                }
            }
            //等待队列中删除该事务
            Long uid = waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
            if (uid != null) removeFromList(wait,uid,xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * 从 List 开头开始尝试解锁，还是个公平锁。解锁时，将该 Lock 对象 unlock 即可，这样业务线程就获取到了锁，就可以继续执行了。
     */
    private void selectNewXID(long uid) {
        //此uid不再被占用，从u2x中移除
        u2x.remove(uid);
        //去该uid的等待列表
        List<Long> waitXids = wait.get(uid);
        //没有xid请求该uid
        if (waitXids == null || waitXids.size() == 0) return;
        while (waitXids.size() > 0){
            //取出一个等待uid的xid
            Long waitXid = waitXids.remove(0);
            // 请求该资源既要存在于wait也要存在于waitLock
            if (!waitLock.containsKey(waitXid)){
                continue;
            }else {
                //将该uid分配给xid
                u2x.put(uid,waitXid);
                Lock lo = waitLock.remove(waitXid);
                waitU.remove(waitXid);
                x2u.get(waitXid).add(uid);
                lo.unlock();
                break;
            }
        }
        if (waitXids.size() == 0) waitXids.remove(uid);
    }

    /**
     * 将id2在key为id1的list中删除
     * 将id2这个资源在id1所持有的资源名单中删除
     * 或者将id2在id1这个资源的等待队列中删除
     */

    private void removeFromList(Map<Long, List<Long>> listMap, long id1, long id2) {
        List<Long> l = listMap.get(id1);
        if (l == null || l.isEmpty()) return;
        Iterator<Long> iterator = l.iterator();
        while(iterator.hasNext()){
            long id = iterator.next();
            if (id == id2){
                iterator.remove();
                break;
            }
        }

        //若删除后list为空，map会删掉这个key
        if (l.size() == 0){
            listMap.remove(id1);
        }
    }

    /**
     * 将id2加入到key为id1的list中
     * 将id2这个资源加入到id1所持有的资源名单中
     * 或者将id2加入到id1这个资源的等待队列中
     */
    private void putInList(Map<Long, List<Long>> listMap, long id1, long id2) {
        if (!listMap.containsKey(id1)){
            listMap.put(id1,new ArrayList<>());
        }
        listMap.get(id1).add(0,id2);
    }

    /**
     * 判断id2是是否在key为id1的list中
     * 判断id2这个资源是否已经在id1所持有的资源名单中
     * 或者判断id2是否在id1这个资源的等待队列中
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long id1, long id2) {
        List<Long> resources = listMap.get(id1);
        if (resources == null || resources.isEmpty()) return false;
        for (long resource : resources) {
            if (resource == id2){
                return true;
            }
        }
        return false;
    }
}
