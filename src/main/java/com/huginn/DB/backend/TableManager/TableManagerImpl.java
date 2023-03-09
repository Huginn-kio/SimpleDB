package com.huginn.DB.backend.TableManager;

import com.huginn.DB.backend.DataManager.DataManager;
import com.huginn.DB.backend.TableManager.parser.statement.*;
import com.huginn.DB.backend.VersionManager.VersionManager;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 由于 TableManager 已经是直接被最外层 Server 调用（MYDB 是 C/S 结构），这些方法直接返回执行的结果，例如错误信息或者结果信息的字节数组（可读）
 *
 */
public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;                                                // TBM的启动类,来管理 MYDB 的启动信息 —— 头表的 UID
    private Map<String, Table> tableCache;                                // 存放TBM管理的所有表信息
    private Map<Long, List<Table>> xidTableCache;                         // 记录了各xid事务所创建的表
    private Lock lock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        this.lock = new ReentrantLock();
        loadTables();
    }

    /**
     * 加载所有的table
     * 因为TBM使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的UID，
     * 所以先获取第一个表的uid然后加载这个表然后可以获得下一个表的uid，然后继续加载这个table直至结束
     */
    private void loadTables() {
       long uid = firstTableUid();
       while(uid != 0){
           Table tb = Table.loadTable(this,uid);
           uid = tb.nextUid;
           tableCache.put(tb.name,tb);
       }
    }

    /**
     * 从启动类中获取首表的uid
     */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /**
     * 修改启动类存放的首表的uid
     */
    private void updateFirstTableUid(long uid){
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }

    /**
     * 提供给用户的接口, 开启一个事务
     */
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;  //begin类指定了隔离级别的类型
        res.xid = vm.begin(level);   //开启了一个事务
        res.result = "begin".getBytes();
        return res;
    }

    /**
     * 提交一个事务
     */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    /**
     * 中止一个事务
     */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    /**
     * 展示所有的table
     */
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if(t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }

    }

    /**
     * 创建新表
     */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                //想要创建的表名已存在
                throw Error.DuplicatedTableException;
            }

            //在创建新表时，采用的时头插法，所以每次创建表都需要更新 Booter 文件
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {             //记录xidTableCache记录了xid事务所创建的表
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 往表中插入数据
     */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    /**
     * 查询表中数据
     */
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    /**
     * 更新表中数据
     */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    /**
     * 删除表中数据
     */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }
}