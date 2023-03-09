package com.huginn.DB.backend.utils;

import com.google.common.primitives.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 工具类，实现基本数据类型和字节数组的转换
 */

public class Parser {

    //short -> byte[]
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    //byte[] -> short
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    //int -> byte[]
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    //byte[] -> int
    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    //long -> byte[]
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    //byte[] -> long
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    //将byte[]解析成ParseStringRes对象 — 即表中字段的字符串类型
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    //将str解析成byte[] —— [strLen][strBytes]
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    //将str转化成long类型的uid
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

}
