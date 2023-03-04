package com.huginn.DB.backend.DataManager.page;

/**
 * dm对底层文件系统读取进缓存的基本单位 —— 页，这个页面是存储在内存中
 */

public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
