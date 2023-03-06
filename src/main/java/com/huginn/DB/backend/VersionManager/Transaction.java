package com.huginn.DB.backend.VersionManager;

import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * VM对一个事务的抽象,以保存快照数据
 */

public class Transaction {

    public long xid;                                                // 该事务的xid
    public int level;                                               // 该事务所处的隔离级别
    public Map<Long, Boolean> snapshot;                             // 快照, 保存事务创建时的事务活跃名单
    public Exception err;                                           //
    public boolean autoAborted;                                     //

    /**
     * 创建一个transaction
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active){
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if (level != 0){
            t.snapshot = new HashMap<>();
            for (Long x : active.keySet()) {
                t.snapshot.put(x,true);
            }
        }

        return t;
    }

    /**
     * 判断xid是否在当前transaction创建时的ReadView中的 active transactions list
     */
    public boolean isInSnapshot(long xid){
        if (xid == TransactionManagerImpl.SUPER_XID) return false;
        return snapshot.containsKey(xid);
    }

}
