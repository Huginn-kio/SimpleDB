package com.huginn.DB.backend.server;

import com.huginn.DB.backend.TableManager.BeginRes;
import com.huginn.DB.backend.TableManager.TableManager;
import com.huginn.DB.backend.TableManager.parser.Parser;
import com.huginn.DB.backend.TableManager.parser.statement.*;
import com.huginn.DB.common.Error;

/**
 * 处理SQL语句
 * Executor 调用 Parser 获取到对应语句的结构化信息对象，并根据对象的类型，调用 TBM 的不同方法进行处理。
 */

public class Executor {
    private long xid;        //所属事务，默认为0 即 自动提交
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    /**
     * 中断执行器(一般系统发生错误时调用)
     */
    public void close(){
       if (xid != 0){                             //执行器中断时时需要把该事务设置成abort即中断该事务
           System.out.println("Abnormal Abort: " + xid);
           tbm.abort(xid);
       }
    }

    /**
     * 执行sql语句并返回结果  (针对事务相关sql语句)
     */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);                      //将sql statement 解析成 对应的类
        if (Begin.class.isInstance(stat)){                    //如果statement是begin语句    开启事务
            if (xid != 0){                                    //在事务内再开启事务  不支持嵌套事务
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)stat);              //开启一个事务
            xid = r.xid;
            return r.result;

        } else if(Commit.class.isInstance(stat)) {            // 提交事务
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;

        } else if(Abort.class.isInstance(stat)) {              //中止事务
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);                             //和表相关操作
        }
    }

    /**
     * 执行sql语句并返回结果  (针对操作表相关sql语句)
     */

    public byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;

        if(xid == 0) {                                                  //当默认是自动提交事务，会创建一个事务去执行，然后系统在执行完毕后自动提交
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {                                      //当默认是自动提交事务, 系统在执行完毕后自动提交
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}