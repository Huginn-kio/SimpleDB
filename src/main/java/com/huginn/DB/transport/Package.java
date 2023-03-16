package com.huginn.DB.transport;

/**
 * 传输的最基本结构
 * HUGDB使用了一种特殊的二进制格式，用于客户端和服务端通信
 */

public class Package {
    byte[] data;
    Exception err;


    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }

}
