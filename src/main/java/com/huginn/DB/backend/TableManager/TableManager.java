package com.huginn.DB.backend.TableManager;

import com.huginn.DB.backend.DataManager.DataManager;
import com.huginn.DB.backend.TableManager.parser.statement.*;
import com.huginn.DB.backend.VersionManager.VersionManager;
import com.huginn.DB.backend.utils.Parser;

public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    /**
     * 创建一个tbm
     */
    public static TableManager create(String path, VersionManager vm, DataManager dm){
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));  //新建的TableManager的首表uid初始化为0
        return new TableManagerImpl(vm,dm,booter);
    }

    /**
     * 加载一个tbm
     */
    public static TableManager open(String path, VersionManager vm, DataManager dm){
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm,dm,booter);
    }
}