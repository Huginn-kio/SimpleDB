package com.huginn.DB.backend.DataManager.page;

import com.huginn.DB.backend.DataManager.pageCache.PageCache;
import com.huginn.DB.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 数据库第一页 —— 是数据库磁盘的页面在cache中的封装
 * 数据库文件的第一页，通常用作一些特殊用途，比如存储一些元数据，用来启动检查什么的。
 * HugDB 的第一页，只是用来做启动检查。
 * 具体的原理是，在每次数据库启动时，会生成一串随机字节，存储在 100 ~ 107 字节。
 * 在数据库正常关闭时，会将这串字节，拷贝到第一页的 108 ~ 115 字节。
 * 数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。如果是异常关闭，就需要执行数据的恢复流程。
 * 页面大小: 8kb
 */

public class PageOne {
    private static final int OF_VC = 100;        //验证码的偏移
    private static final int LEN_VC = 8;         //验证码的长度


    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     *数据库正常启动时，会生成一串随机字节，存储在 100 ~ 107 字节，此时页面为脏页
     * Page是读入到内存中的，在内存中对该页进行验证码的写入
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        //生成随即字节并写入对应位置, Page中的data部分才是存储在数据库中
        setVcOpen(pg.getData());
    }

    /**
     * Page是数据库页在内存中的封装，data部分才是真正存储在磁盘中的数据
     */
    private static void setVcOpen(byte[] raw) {
        //随机生成8字节验证码，将其拷贝raw的OF_VC处
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }
    /**
     * 数据库正常关闭时，会将验证码写在存储在108 ~ 115字节，此时页面为脏页
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        //写入对应位置
        setVcClose(pg.getData());
    }

    /**
     * 在数据库正常关闭时，会将验证码，拷贝到第一页的 108 ~ 115 字节。
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    //判断上一次是否正常关闭
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }


}
