package com.huginn.DB.backend.common;

/**
 * 定义一个子数组类，并规定该子数组的访问范围，来实现共享内存数组
 * 因为java中如果使用SubArray来定义一个新的子数组，不会使用原来的空间，而且会开一个新的内存空间并进行复制
 */

public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
