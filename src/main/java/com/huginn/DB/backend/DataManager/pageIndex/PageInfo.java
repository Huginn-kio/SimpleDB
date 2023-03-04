package com.huginn.DB.backend.DataManager.pageIndex;

/**
 * 记录了某一页的空闲空间的信息
 */

public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
