package com.huginn.DB.backend.DataManager.page;

import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.DataManager.pageCache.PageCacheImpl;
import com.huginn.DB.backend.utils.Parser;

import java.util.Arrays;

/**
 * 普通页 — 数据库文件的普通页在cache中的封装抽象
 * 普通页的结构： 2字节无符号数(表这一页的空闲位置偏移) + 实际存储的数据  ——>  2字节(存了offset的值) + 存储的数据 + offset空闲位置
 * 对普通页的管理，基本围绕FSO(Free Space Offset)进行
 * 页面大小：8kb
 */
public class PageX {
    private static final short OF_FREE = 0;                                         //偏移开始
    private static final short OF_DATA = 2;                                         //偏移字节
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;         //剩余的数据空间


    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    //向页面pg插入数据raw
    public static short insert(Page pg, byte[] raw){
        pg.setDirty(true);
        //得到空闲位置的偏移(最大为32767，可以表示8000)
        short offset = getFSO(pg.getData());
        //往空闲的数据空间插入数据raw
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        //重新设置offset
        setFSO(pg.getData(),(short)(offset + raw.length));
        return offset;
    }

    //重新设置offset的值
    private static void setFSO(byte[] raw, short offset) {
        System.arraycopy(Parser.short2Byte(offset),0,raw,OF_FREE,OF_DATA);
    }


    //得到页面的2字节存的空闲位置偏移值 —— 已使用的空间
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    //得到页面的2字节偏移值 —— 已使用的空间
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    //获取页面的空闲空间大小
    public static int getFreeSpace(Page pg){
        return PageCacheImpl.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * 剩余两个函数 recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用。
     */

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        //rawFSO是原来未插入前的page的FSO
        short rawFSO = getFSO(pg.getData());

        //插入前的内容 < 插入后的内容, 内容增加，FSO增加
        if (rawFSO < offset + raw.length){
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    // 将raw插入pg中的offset位置，只更新值,内容大小不发生改变不更新FSO
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
