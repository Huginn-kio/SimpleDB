package com.huginn.DB.backend.VersionManager;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.common.SubArray;
import com.huginn.DB.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry —— 存储在DataItem中的data部分
 * entry: [XMIN] [XMAX] [data]
 * XMIN: 创建该记录(版本)的事务编号
 * XMAX: 删除该条记录(版本)的事务编号
 * DATA: 这条记录持有的数据
 * XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写。
 * VM 并没有提供 Update 操作，对于字段的更新操作由后面的表和字段管理（TBM）实现。所以在 VM 的实现中，一条记录只有一个版本
 * 一条记录存储在一条 DataItem 中，所以 Entry 中保存一个 DataItem 的引用
 */

public class Entry {
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;         // 8bit分别记录XMIN和XMAX
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;                                       //DataItem的id
    private DataItem dataItem;                              //记录存储的DataItem
    private VersionManager vm;

    /**
     * 生成entry
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }


    /**
     * 根据DataItem的uid加载一个entry
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 移除一个entry
     */
    public void remove() {
        dataItem.release();
    }

    /**
     * 释放entry
     */
    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }


    /**
     * 生成entry格式的byte[] [XMIN] [XMAX] [data]
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data){
        //生成[XMIN]
        byte[] xmin = Parser.long2Byte(xid);
        //生成[XMAX]
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin,xmax,data);
    }

    /**
     * 返回该entry的data部分
     * 一条entry存储在一条DataItem中
     */
    public byte[] data(){
        //加读锁
        dataItem.rLock();
        try{
            //DataItem的data部分 —— 即entry
            SubArray sa = dataItem.data();
            //entry的data部分
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data,0,data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 给entry设置XMAX，因为entry是DataItem的数据部分，相当于修改DataItem所以需要先执行before
     */

    public void setXmax(long xid){
        dataItem.before();
        try{
            //DataItem的data部分 —— 即entry
            SubArray sa = dataItem.data();
            //entry的data部分
            System.arraycopy(Parser.long2Byte(xid), 0,sa.raw, sa.start + OF_XMAX, 8);
        }finally {
            dataItem.after(xid);
        }
    }

    /**
     * 获取XMIN
     */
    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 获取XMAX
     */
    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    /**
     * 返回uid
     */
    public long getUid() {
        return uid;
    }
}
