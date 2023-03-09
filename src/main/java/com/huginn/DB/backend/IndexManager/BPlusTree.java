package com.huginn.DB.backend.IndexManager;

import com.huginn.DB.backend.DataManager.DataManager;
import com.huginn.DB.backend.DataManager.DataManagerImpl;
import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;
import com.huginn.DB.backend.common.SubArray;
import com.huginn.DB.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * B+树
 * IM 对上层模块主要提供两种能力：插入索引和搜索节点。
 * IM 为什么不提供删除索引的能力?
 * 当上层模块通过 VM 删除某个 Entry，实际的操作是设置其 XMAX。
 * 如果不去删除对应索引的话，当后续再次尝试读取该 Entry 时，是可以通过索引寻找到的，
 * 但是由于设置了 XMAX，寻找不到合适的版本而返回一个找不到内容的错误。
 */

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;   //由于 B+ 树在插入删除时，会动态调整，根节点不是固定节点，于是设置一个 bootDataItem，该 DataItem 中存储了根节点的 UID。
    Lock bootLock;

    /**
     * 创建B+树，并返回其bootDataItem的uid
     */
    public static long create(DataManager dm) throws Exception {
        //创建root节点
        byte[] rawRoot = Node.newNilRootRaw();
        //插入root节点，并返回其uid
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        //将根节点的uid插入，并返回该DataItem的uid(这个DataItem应该是bootDataItem)
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /**
     * 根据bootUid加载一个B+树
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }


    /**
     * 获取根节点的uid
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();  //dataItem是一个操作单元，里面可以存各种数据不一定是entry，这里的bootDataItem的数据部分用来存root的id
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新根节点的Uid
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            //更新根节点
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            //通过0事务对该节点进行插入，并返回该节点的uid
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            //在修改DataItem前需要调用其before方法 — 写锁 设置脏页 保存前相数据
            bootDataItem.before();
            SubArray bootDataItemRaw = bootDataItem.data();
            //修改rootDataItem的内容 —— root的uid
            System.arraycopy(Parser.long2Byte(newRootUid), 0, bootDataItemRaw, bootDataItemRaw.start, 8);
            //落日志，释放锁, 注意到 IM 在操作 DM 时，使用的事务都是 SUPER_XID
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 根据nodeUid和key找叶节点
     * 如果nodeUid对应的是叶节点，则返回
     * 否则根据key查找叶节点
     * 最终会返回key存在的叶子节点
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            return nodeUid;  //nodeUid对应的节点是叶节点则返回
        } else {
            long next = searchNext(nodeUid, key);  //寻找下一个节点
            return searchLeaf(next, key);
        }
    }

    /**
     * 查找并返回key存在的下一个节点
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key); //这里返回的要么是下一个该key的下一个节点，要么是这个节点找不到该key返回的兄弟节点
            node.release();
            //返回key存在的下一个节点
            if (res.uid != 0) return res.uid;
            //key不在该节点上，去兄弟节点进行查找
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 返回满足key的节点的uid
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     * 返回所有满足于此区间的所有叶子节点的uid
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        //找到leftKey存在的叶子节点,叶子节点之间有序且用链表连接，可以根据链表进行查找
        long leafUid = searchLeaf(rootUid, leftKey);
        //这里存储所有[leftKey, rightKey]的叶子节点uid
        List<Long> uids = new ArrayList<>();
        while (true) {
            Node leaf = Node.loadNode(this, leafUid);
            //返回这个[leftKey, rightKey]的叶子节点uid, 以及可能还有key满足这个区间的兄弟节点uid
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            //判断兄弟uid是否为0，若为0则说明兄弟节点不存在满足区间的key，否则需要到兄弟节点继续进行查找
            if (res.siblingUid != 0) {
                leafUid = res.siblingUid;
            } else {
                break;
            }
        }
        return uids;
    }

    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 插入节点
     */
    public void insert(long key, long uid) throws Exception {
        //get root uid of b+ tree
        long rootUid = rootUid();
        //从根节点开始进行key的插入
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        //这种情况是，根节点分裂了
        if (res.newNode != 0){
            updateRootUid(rootUid,res.newNode,res.newKey);
        }
    }

    /**
     *  在nodeUid的节点开始插入key(叶子节点和中间节点)
     */
    public InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this,nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if (isLeaf){
            //叶子节点插入值, res含有新节点或者空信息
            res = insertAndSplit(nodeUid, uid, key);
        }else {
            // this is not a leaf node
            // 非叶子节点
            long next = searchNext(nodeUid, key);
            // 递归调用 —— 去找下一个节点进行插入 —— 直到找到叶子节点进行插入返回res
            InsertRes ir = insert(next,uid,key);
            // 返回的ir的newNode要么是0(直接插入不用分裂)要么是新节点的uid(进行了分裂)
            if (ir.newNode != 0){
                // 结点进了分裂但是上面的key, 在这个节点插入这个key
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            }else {
                // 没有分裂
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 在nodeUid的节点开始找到位置插入key
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true){
            Node node = Node.loadNode(this,nodeUid);
            // 插入节点并得到相关的数据
            // 三种情况: 1. key不在node的key范围返回兄弟节点uid 2. 在node插入key需要分离的话会返回新节点的uid和首key 3. 在node插入key不需要分裂返回0
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();

            //第一种情况,返回兄弟节点uid
            if (iasr.siblingUid != 0){
                // 在下一次循环会进入该节点进行查找
                nodeUid = iasr.siblingUid;
            }else {
                //第二第三种情况
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }

    /**
     * 释放该B+树
     */
    public void close(){
        bootDataItem.release();
    }
}
