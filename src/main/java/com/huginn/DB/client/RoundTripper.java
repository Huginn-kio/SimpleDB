package com.huginn.DB.client;

import com.huginn.DB.transport.Package;
import com.huginn.DB.transport.Packager;

import java.io.IOException;

/**
 * RoundTripper 类实际上实现了单次收发动作
 */

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 实现一次收发
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }


    public void close() throws IOException {
        packager.close();
    }
}
