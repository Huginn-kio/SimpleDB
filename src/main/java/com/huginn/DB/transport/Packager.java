package com.huginn.DB.transport;

import java.io.IOException;

/**
 * Encoder 和 Transporter 的结合体，直接对外提供 send 和 receive 方法    编码 + 发送   接收 + 解码
 */

public class Packager {
    private Transporter transporter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transporter = transporter;
        this.encoder = encoder;
    }

    /**
     * 接收一个package
     */
    public Package receive() throws Exception {
        byte[] data = transporter.receive();
        return encoder.decode(data);
    }

    /**
     * 发送一个package
     */
    public void send(Package pkg) throws IOException {
        byte[] data = encoder.encode(pkg);
        transporter.send(data);
    }

    /**
     * 关闭Packager
     */
    public void close() throws IOException {
        transporter.close();
    }
}
