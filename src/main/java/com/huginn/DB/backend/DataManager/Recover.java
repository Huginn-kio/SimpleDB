package com.huginn.DB.backend.DataManager;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.DataManager.dataItem.DataItem;
import com.huginn.DB.backend.DataManager.logger.Logger;
import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.DataManager.page.PageX;
import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.TransactionManager.TransactionManager;
import com.huginn.DB.backend.common.SubArray;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.util.*;

public class Recover {
    //两种日志的格式
    //insert log: [LogType] [XID] [Pgno] [Offset] [Raw]
    //update log: [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final byte LOG_TYPE_INSERT = 0;     //insert log
    private static final byte LOG_TYPE_UPDATE = 1;     //update log
    private static final int REDO = 0;
    private static final int UNDO = 1;


    /**
     * recover分两步
     * 1. redo所有已完成事务 committed aborted
     * 2. undo所有未完成事务 active
     */
    public static void recover(TransactionManager tm, Logger logs, PageCache pc){
        System.out.println("Recovering the database ...");
        //定位到日志文件的第一条日志的开头
        logs.rewind();
        //遍历所有的日志，找到最大的page号数，由于日志记载了用户所有的对数据库的操作，这里的maxPgno即历史上用户对数据库操作的最大的页数
        //maxPgno后面的页数都是没有日志记录的数据，用于后面截取文件 —— 截掉多余的数据
        int maxPgno = 0;
        while(true) {
            byte[] log = logs.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }

        //todo
        //根据日志中出现的最大的页数截取文件
        pc.truncateByPgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, logs, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, logs, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    /**
     * redo所有已完成事务
     * 1. 逐一遍历日志
     * 2. 检查日志对应的事务是否已完成，如果已完成，则redo
     */
    private static void redoTransactions(TransactionManager tm, Logger logs, PageCache pc){
        //定位到第一条日志之前
        logs.rewind();

        //redo所有非active事务
        while (true){
            //逐一读取日志
            byte[] log = logs.next();
            if (log == null) break;
            //判断是否是插入型日志
            if (isInsertLog(log)){
                InsertLogInfo iLog = parseInsertLog(log);
                long xid = iLog.xid;
                //判断该日志的事务是否已经完成
                if (!tm.isActive(xid)){
                    //重做该日志记录的插入操作
                    doInsertLog(pc,log,REDO);
                }
            }else {
                //this is a update
                UpdateLogInfo uLog = parseUpdateLog(log);
                long xid = uLog.xid;
                if (!tm.isActive(xid)){
                    doUpdateLog(pc,log,REDO);
                }
            }
        }
    }

    /**
     * undo所有已完成事务
     * 因为多线程下，会存在多个active transactions
     * 1. 查看日志最后一个事务Ti
     * 1. 逐一遍历日志
     * 2. 检查日志对应的事务是否未完成，如果未完成，则将其加入到map中。然后逆序undo这些操作
     */
    private static void undoTransactions(TransactionManager tm, Logger logs, PageCache pc) {
        //记录active transaction和他们对应的logs
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logs.rewind();

        while (true){
            //需要的是log中的数据部分 [LogType] [XID] [Pgno] [Offset] [Raw]
            byte[] log = logs.next();
            if (log == null) break;
            if (isInsertLog(log)){
                InsertLogInfo iLog = parseInsertLog(log);
                long xid = iLog.xid;
                if (tm.isActive(xid)){
                    if (!logCache.containsKey(xid)){
                        logCache.put(xid,new ArrayList<byte[]>());
                    }
                    logCache.get(xid).add(log);
                }
            }else {
                //this is a update
                UpdateLogInfo uLog = parseUpdateLog(log);
                long xid = uLog.xid;
                if (tm.isActive(xid)){
                    if (!logCache.containsKey(xid)){
                        logCache.put(xid,new ArrayList<byte[]>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        //对所有active的事务的log进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            //得到一个active事务的所有logs
            List<byte[]> logsForActiveTransaction = entry.getValue();
            for (int i = logsForActiveTransaction.size() - 1; i >= 0 ; i--) {
                //得到这个事务的最后的一个log的data部分
                byte[] log = logsForActiveTransaction.get(i);
                //进行undo操作
                if (isInsertLog(log)){
                    doInsertLog(pc,log,UNDO);
                }else{
                    doUpdateLog(pc,log,UNDO);
                }
            }

            //将该事务的状态设置为aborted，因为对其进行了undo操作
            tm.abort(entry.getKey());
        }
    }


    /**
     *   insert log 相关操作  [LogType] [XID] [Pgno] [Offset] [Raw]
     */

    //插入型事务结构的信息
    static class InsertLogInfo{
        long xid;       // 事务id
        int pgno;       // pgno + offset 表示插入的位置
        short offset;
        byte[] raw;     //插入的数据信息
    }

    private static final int OF_TYPE = 0;               //type的开始位置为0
    private static final int OF_XID = OF_TYPE + 1;      // xid的开始位置为1, type只有两种所以只需要一位
    private static final int OF_INSERT_PGNO = OF_XID + 8;  //pgno的开始位置, xid为long, 占8位
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4; //offset的开始位置, pgno为int, 占4位
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;  //raw的开始位置, offset为short, 占2位

    /**
     * 判断该事务是否是插入型事务
     */
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    /**
     * 将log解析成InsertLog
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo iLog = new InsertLogInfo();
        iLog.xid = Parser.parseLong(Arrays.copyOfRange(log,OF_XID,OF_INSERT_PGNO));
        iLog.pgno = Parser.parseInt(Arrays.copyOfRange(log,OF_INSERT_PGNO,OF_INSERT_OFFSET));
        iLog.offset = Parser.parseShort(Arrays.copyOfRange(log,OF_INSERT_OFFSET,OF_INSERT_RAW));
        iLog.raw = Arrays.copyOfRange(log,OF_INSERT_RAW,log.length);
        return iLog;
    }

    /**
     * 按照log(插入型)执行插入操作, [LogType] [XID] [Pgno] [Offset] [Raw]
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo iLog = parseInsertLog(log);
        Page pg = null;

        //根据页号获取该页
        try {
            pg = pc.getPage(iLog.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        //判断是redo还是undo
        try {
            if (flag == UNDO){
                DataItem.setDataItemRawInvaild(iLog.raw);
            }else {
                //todo
                PageX.recoverInsert(pg,iLog.raw,iLog.offset);
            }
        } finally {
            pg.release();
        }
    }

    /**
     * 生成一条插入型日志，并返回日志的byte[]
     * [LogType] [XID] [Pgno] [Offset] [Raw]
     */

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }


    /**
     *  update log 相关操作  [LogType] [XID] [uid] [OldRaw] [NewRaw]
     */

    //更新型事务结构的信息
    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    private static final int OF_UPDATE_UID = OF_XID + 8;         //uid的开始位置, xid为int, 占8位
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;  //raw的开始位置, uid为int, 占8位

    /**
     * 判断该事务是否是更新型事务
     */
    private static boolean isUpdateLog(byte[] log) {
        return log[0] == LOG_TYPE_UPDATE;
    }

    /**
     * 将log解析成UpdateLog
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    /**
     * 按照log(更新)执行更新操作
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        UpdateLogInfo uLog = parseUpdateLog(log);
        Page pg = null;

        //根据页号获取该页
        try {
            pg = pc.getPage(uLog.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }

        //判断是redo还是undo
        try {
            if (flag == UNDO){
                PageX.recoverUpdate(pg,uLog.oldRaw,uLog.offset);
            }else {
                //todo
                PageX.recoverUpdate(pg,uLog.newRaw,uLog.offset);
            }
        } finally {
            pg.release();
        }
    }

    /**
     * 生成一条更新型日志，并返回日志的byte[]
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     */

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }
}
