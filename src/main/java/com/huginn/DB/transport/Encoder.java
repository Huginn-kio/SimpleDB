package com.huginn.DB.transport;

import com.google.common.primitives.Bytes;
import com.huginn.DB.common.Error;

import java.util.Arrays;

/**
 * 数据编码
 * 每个 Package 在发送前，由 Encoder 编码为字节数组，在对方收到后同样会由 Encoder 解码成 Package 对象
 * 编码和解码的规则如下：
 * [Flag][data]
 * flag = 0 则 发送的是数据，则data是数据本身
 * flag = 1 则 发送的是错误，则data是err.getMessage()的错误提示信息
 *
 */

public class Encoder {

    /**
     * 编码数据
     */
    public byte[] encode(Package pkg){

        if(pkg.getErr() != null){
            //如果是发送错误信息的包
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null){
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        }else {
            //如果是数据包
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 解码数据
     */
    public Package decode(byte[] data) throws Exception {
        if (data.length < 1)
            throw Error.InvalidPkgDataException;

        if(data[0] == 0){  //数据包
            return new Package(Arrays.copyOfRange(data,1,data.length),null);
        }else if(data[0] == 1){  //错误信息包
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data,1,data.length))));
        }else{
            throw Error.InvalidPkgDataException;
        }
    }
}
