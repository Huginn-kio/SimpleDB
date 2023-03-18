package com.huginn.DB.client;

import com.huginn.DB.transport.Package;

import java.io.IOException;

/**
 * 客户端，执行用户的各种命令
 */

public class Client {
    private RoundTripper rt;

    public Client(RoundTripper rt) {
        this.rt = rt;
    }

    /**
     * 向客户端发送sql
     */
    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat,null); //将收到的sql语句进行打包
        Package resPkg = rt.roundTrip(pkg);       //向服务端发送sql，并返回sql结果
        if (resPkg.getErr() != null){             //返回的是错误
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close(){
        try {
            rt.close();
        } catch (IOException e) {
        }
    }
}
