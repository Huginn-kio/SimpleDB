package com.huginn.DB.backend.VersionManager;

import com.huginn.DB.backend.TransactionManager.TransactionManager;

/**
 * 隔离级别 —— 提供两个级别的隔离级别
 * Read Committed 读提交       0
 * Repeatable Read 可重复读    1
 */

public class Visibility {

    /**
     * 读提交是允许版本跳跃的，而可重复读则是不允许版本跳跃的。
     * 解决版本跳跃的思路也很简单：如果 Ti 需要修改 X，而 X 已经被 Ti 不可见的事务 Tj 修改了并提交了，那么要求 Ti 回滚。
     * Ti 不可见的 Tj，有两种情况：
     * 1. XID(Tj) > XID(Ti)
     * 2. Tj in SP(Ti)
     * 版本跳跃的检查: 取出要修改的数据 X 的最新提交版本，并检查该最新版本的创建者对当前事务是否可见
     */
     public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e){
         // 修改了该记录的xid
         long xmax = e.getXmax();
         if (t.level == 0){
             return false;
         }else {
             //存在versionSkip
             //已提交的最新版本 + (修改的事务是不可见事务，即创建在本事务之后的事务，或本事务创建时的活跃事务
             return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
         }
     }



    /**
     * 读提交，判断某个entry对事务t是否可见
     *
     * entry — XMIN 应当在版本创建时填写，而 XMAX 则在版本被删除，或者有新版本出现时填写
     *
     * 在读提交下, 可见性 =
     * (XMIN == Ti and XMAX == NULL)(本事务创建且未删除) or
     * (XMIN is committed and (XMAX == NULL or (XMAX != Ti and XMAX is not committed)))(创建记录的事务已提交，没有事务对其进行删除或者别的事务对其进行删除但未提交(未提交则不可见))
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        //第一种情况
        if ( xmin == xid && xmax == 0) return true;

        //第二种情况
        if (tm.isCommitted(xmin)){
            if(xmax == 0)  return true;
            if(xmax != xid){
                if (!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读，判断某个entry对事务t是否可见
     *
     * 可重复读 — 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本
     * 这条规定，增加于，事务需要忽略：
     * 1. 在本事务后开始的事务的数据;
     * 2. 本事务开始时还是 active 状态的事务的数据
     * 对于第一条，只需要比较事务 ID，即可确定。
     * 而对于第二条，则需要在事务 Ti 开始时，记录下当前活跃的所有事务 SP(Ti)，如果记录的某个版本，XMIN 在 SP(Ti) 中，也应当对 Ti 不可见。
     *
     * 可重复读下, 可见性 =
     * (XMIN == Ti and XMAX == NULL)(本事务创建且未删除) or
     * (XMIN is committed and (XMIN < XID and XMIN is not in SP(Ti) and (XMAX == NULL or (XMAX != Ti and XMAX is not committed or XMAX > Ti or XMAX is in SP(Ti)) ))
     * (创建记录的事务已提交, 创建该记录的事务小于xid且该事务不在sp(确保该记录在xid创建前已经提交),
     * 该记录未被删除 或 删除该记录的事务未提交 或 删除该记录的事务在xid之后创建 或 删除该记录的事务在创建时的活跃名单)
     */

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        //第一种情况
        if ( xmin == xid && xmax == 0 ) return true;

        //第二种情况
        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            if(xmax == 0)  return true;
            if(xmax != xid){
                if (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断entry对于transaction t是否可见
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry entry) {
        if (t.level == 0){
            //读已提交
            return  readCommitted(tm, t, entry);
        }else {
            //可重复读
            return repeatableRead(tm, t, entry);
        }
    }
}
