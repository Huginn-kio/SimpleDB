package com.huginn.DB.backend.VersionManager;

/**
 * VersionManager
 */

public interface VersionManager {

    //向上层提供功能
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    //Transaction相关，transaction用于检查和快照使用
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);
}
