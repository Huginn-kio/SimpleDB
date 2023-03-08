package com.huginn.DB.backend.IndexManager;

import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;
import com.huginn.DB.backend.common.SubArray;
import com.huginn.DB.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * B+树的一个结点，每个 Node 都存储在一条 DataItem 中
 * Node结构：[LeafFlag][KeyNumber][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * LeafFlag: 标记了是否是个叶子节点
 * KeyNumber: 节点的key个数
 * SiblingUid: 兄弟节点存储在DM中的UID
 * SonN & KeyN: 子节点和对应的Key (每一个节点的KeyN都是Long.MAX_VALUE)
 * Node 类持有了其 B+ 树结构的引用，DataItem 的引用和 SubArray 的引用，用于方便快速修改数据和释放数据。
 */

public class Node {
    //[LeafFlag][KeyNumber][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]
    //    1          2          8
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);  //理论上可以容纳(BALANCE_NUMBER * 2)个key and child

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 生成一个根节点的数据
     */
    static byte[] newRootRaw(long left, long right, long key){
        //raw是node节点的整个数据结构 [LeafFlag][KeyNumber][SiblingUid][Son0][Key0][Son1][Key1]...[SonN][KeyN]
        SubArray raw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);

        //根节点非叶子节点
       setRawIsLeaf(raw,false);
       //根节点初始化两个子节点left和right
       setRawNoKeys(raw,2);
       //无兄弟节点，所以设置为uid = 0
       setRawSibling(raw,0);
       //两个子节点，分别是left和right, left为第0个son，right为第1个son
       setRawKthSon(raw,left,0);
       setRawKthKey(raw,key,0);  //这个传进来的key是左子节点的key
       setRawKthSon(raw,right,1);
       setRawKthKey(raw,Long.MAX_VALUE,1);

       return raw.raw;
    }

    /**
     * 生成一个空的根节点数据
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);
        setRawIsLeaf(raw,true);
        setRawNoKeys(raw,0);
        setRawSibling(raw,0);
        return raw.raw;
    }

    /**
     * 给Node节点设置LeafFlag字段
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf){
            //如果是叶子节点，该1字节LeafFlag数据为1
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        }else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    /**
     * 获得节点的判断是否叶子节点的信息
     */
    static boolean getRawLeaf(SubArray raw){
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /**
     * 给Node节点设置KeyNumber字段
     */
    static void setRawNoKeys(SubArray raw, int noKeys){
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET,  2);
    }

    /**
     * 获得节点的key数量
     */
    static int getRawNoKeys(SubArray raw){
        return (int) Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + 2));
    }

    /**
     * 给Node节点设置Sibling字段
     */
    static void setRawSibling(SubArray raw, long sibling){
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    /**
     * 获得该节点的兄弟节点的uid
     */
    static long getRawSibling(SubArray raw){
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + 8));
    }

    /**
     * 给Node节点设置kth的子节点
     */
    static void setRawKthSon(SubArray raw, long uid, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    /**
     * 获得该节点的kth的子节点
     */
    static long getRawKthSon(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 给Node节点设置kth的key
     */
    static void setRawKthKey(SubArray raw, long key, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    /**
     * 获得节点kth子节点的key
     */
    static long getRawKthKey(SubArray raw, int kth){
        int offset = raw.start + NODE_HEADER_SIZE + kth * (8 * 2) + 8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    /**
     * 从B+树中加载uid的node
     */
    public static Node loadNode(BPlusTree tree, long uid) throws Exception {
        //每个node都存储在一个DataItem中
        DataItem di = tree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = tree;
        n.dataItem = di;
        n.uid = uid;
        n.raw = di.data();
        return n;
    }

    /**
     * 释放一个node节点，本质上是释放存放它的DataItem
     */
    public void release(){
        dataItem.release();
    }

    /**
     * 判断node节点是否是叶子节点
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * B+树索引插入new key, 然后按需进行分裂节点,返回res
     * 三种情况:
     * 1. key不在该节点的范围  res含有sonlingUid
     * 2. key在节点范围插入本节点不用分裂    res只含默认值(0)
     * 3. key在节点范围插入本节点进行分裂    res含有newSon(新节点uid) newKey(新节点首key)
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
       boolean success = false;
       Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            //insert data into this node
            success = insert(uid,key);
            if (!success){
                //need to go to sibling node
                //get sibling uid
                res.siblingUid = getRawSibling(raw);
                return res;
            }

            // judge whether this node need to be split
            if (needSplit()){
                try {
                    // split this node and return new node
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            }else {
                //this node does not need to be split
                return res;
            }
        } finally {
            if (err == null && success){
                //insert successfully and no error
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            }else {
                //error and rollback
                dataItem.unBefore();
            }
        }
    }

    /**
     * 判断本node是否需要分裂，理论上key可以到达 2 * BALANCE_NUMBER
     */
    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    class SplitRes{
        long newSon, newKey;
    }

    /**
     * 对节点进行分裂
     */
    private SplitRes split() throws Exception {
        //分裂节点需要新建一个节点
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE],0,NODE_SIZE);
        //当节点到达BALANCE_NUMBER就进行分裂，需要把一半的key分配过新的node
        //如果本节点是叶子节点,那么分裂出来的节点就是叶子节点,如果不是,新节点也不是
        setRawIsLeaf(nodeRaw,getRawLeaf(raw));
        setRawNoKeys(nodeRaw,BALANCE_NUMBER);
        //将本节点的兄弟变成它的兄弟,然后本节点的兄弟改成这个节点
        setRawSibling(nodeRaw,getRawSibling(raw));
        //将本节点BALANCE_NUMBER之后的key和son全部拷贝到新的节点中
        copyRawFromKth(raw,nodeRaw,BALANCE_NUMBER);
        //将新节点封装为DataItem保存并返回其uid
        long sibling = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        //前面拷贝了BALANCE_NUMBER之后的key和son给新节点,但没有在本节点抹除这些信息,通过设置总key数就可以解决(后面的数据仍存在,但属于是无用数据)
        setRawNoKeys(raw,BALANCE_NUMBER);
        //将本节点的兄弟设置为新节点(新节点的兄弟是本节点原来的兄弟)
        setRawSibling(raw,sibling);

        //保存分离数据
        SplitRes res = new SplitRes();
        res.newSon = sibling;   // 新节点的uid
        res.newKey = getRawKthKey(raw,0);  //新节点的首key
        return res;
    }



    /**
     * 将uid和key插入到节点中
     * true : key位于本节点范围，将其插入本节点
     * false ： key不在本节点范围，需要到兄弟节点进行插入
     */

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKeys){
            long ik = getRawKthKey(raw,kth);
            if (ik < key){
                kth++;
            }else {
                break;
            }
        }
        //kthKey >= key or key > all keys in this node
        //key > all keys in this node, and node has sibling
        if (kth == noKeys && getRawSibling(raw) != 0) return false;

        // key <= kth Key
        if (getRawLeaf(raw)){
            //this node is a leaf node
            // shift Kth and Kth+ data to insert data into kth position
            shiftRawKth(raw,kth);
            //insert data into kth position
            setRawKthKey(raw,key,kth);
            setRawKthSon(raw,uid,kth);
            setRawNoKeys(raw,noKeys+1);
        }else {
            // is not a leaf node
            //get kth key
            long kKey = getRawKthKey(raw, kth);
            //todo
            //key -> kth
            setRawKthKey(raw,key,kth);
            shiftRawKth(raw,kth + 1);
            //kth key -> kth + 1 key
            setRawKthKey(raw,kKey,kth + 1);
            setRawKthSon(raw,uid, kth + 1);
            setRawNoKeys(raw,noKeys + 1);
        }
        return true;
    }

    /**
     * 从kth + 1开始向后偏移一个[kson kkey]
     * kth及其之前的数据不变，kth + 1的数据变成kth的数据,....
     * 为了方便在kth这个位置插入
     */
    static void shiftRawKth(SubArray raw, int kth) {
       int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
       int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; i--) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /**
     * 从from中copy kth之后的key和son 到 to中
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * Node 类有两个方法，用于辅助 B+ 树做插入和搜索操作，分别是 searchNext 方法和 leafSearchRange 方法
     */

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * searchNext 寻找对应 key 的 UID, 如果找不到, 则返回兄弟节点的 UID
     */
    public SearchNextRes searchNext(long key){
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            //得到这个node的key数量
            int noKeys = getRawNoKeys(raw);
            //遍历它的keys(从小到大排序), Kth Son上的Keys都小于 Kth Key, 如果要查找的key< kth Key. 那么要查找的key位于kth Son
            for (int i = 0; i < noKeys; i++) {
                long keys = getRawKthKey(raw,i);
                if (key < keys){
                    //要查找的key位于该子节点,返回其uid
                    res.uid = getRawKthSon(raw,i);
                    res.siblingUid = 0;
                    return res;
                }
            }

            //在该节点上找不到该key的node，去其兄弟节点上查找
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;  //范围查找满足的uid
        long siblingUid;
    }

    /**
     * 这是叶节点通过链表进行查找范围内的所有叶节点uid
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]，这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();

        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            //找到第一个位于[lk,rk]区间的key
            while(kth < noKeys){
                long keys = getRawKthKey(raw,kth);
                if (keys >= leftKey)
                    break;
                kth++;
            }

            List<Long> uids = new ArrayList<>();
            while(kth < noKeys){
                //收集位于这个区间的uids
                long keys = getRawKthKey(raw,kth);
                if (keys <= rightKey){
                    uids.add(getRawKthSon(raw,kth));
                    kth++;
                }else {
                    break;
                }
            }

            long siblingUid = 0;
            //这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点
            if (kth == noKeys){  //证明rightKey >= 所有keys
                siblingUid = getRawSibling(raw);
            }

            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
