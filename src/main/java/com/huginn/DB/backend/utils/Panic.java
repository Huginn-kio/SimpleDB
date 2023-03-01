package com.huginn.DB.backend.utils;

/**
 * 定义如何统一处理异常，打印错误信息然后强制停机
 */

public class Panic {
    public static void panic(Exception err){
        err.printStackTrace();
        System.exit(1);
    }
}
