package com.huginn.DB.backend.DataManager.dataItem;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.DataManager.DataManagerImpl;
import com.huginn.DB.backend.DataManager.page.Page;
import com.huginn.DB.backend.common.SubArray;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.backend.utils.Types;

import java.util.Arrays;

public interface DataItem {

    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    /**
     * 从页面的offset处读取数据封装成DataItem
     */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        //得到DataItem所在的页数
        byte[] data = pg.getData();
        //解析获得dataItem的data大小，通过pg和offset到达该DataItem的位置，根据[ValidFlag] [DataSize] [Data]的分布读出size
        short dataSize = Parser.parseShort(Arrays.copyOfRange(data,offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        //读取整一个DataItem
        SubArray raw = new SubArray(data,offset,offset + DataItemImpl.OF_DATA + dataSize);
        //获取uid
        long uid = Types.addressToUid(pg.getPageNumber(),offset);
        return new DataItemImpl(raw, new byte[dataSize + DataItemImpl.OF_DATA], pg, uid, dm);
    }

    /**
     * 将data封装陈DataItem格式，因为磁盘中存储的是DataItem格式的数据，即加上valid和size信息
     */

    public static byte[] wrapDataItemRaw(byte[] data) {
        byte[] valid = new byte[1]; //默认是0，即有效
        byte[] size = Parser.short2Byte((short) data.length);
        return Bytes.concat(valid,size,data);
    }

    /**
     * 使该数据无效，即设计DataItem的第一位为1
     */
    static void setDataItemRawInvaild(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }

}
