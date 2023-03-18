package com.huginn.DB.backend;

import com.huginn.DB.backend.DataManager.DataManager;
import com.huginn.DB.backend.TableManager.TableManager;
import com.huginn.DB.backend.TransactionManager.TransactionManager;
import com.huginn.DB.backend.VersionManager.VersionManager;
import com.huginn.DB.backend.VersionManager.VersionManagerImpl;
import com.huginn.DB.backend.server.Server;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;
import org.apache.commons.cli.*;

/**
 * 服务器的启动入口。这个类解析了命令行参数。
 * 很重要的参数就是 -open 或者 -create。
 * Launcher 根据两个参数，来决定是创建数据库文件，还是启动一个已有的数据库。
 */

public class Launcher {
    public static final int port = 9999;
    public static final long DEFAULT_MEM = (1<<20)*64;
    public static final long KB = 1<<10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    //服务端启动程序，解析命令行参数
    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    /**
     * 创建数据库文件
     */
    private static void createDB(String path){
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFAULT_MEM, tm);  //给页缓存分配64MB
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path,vm,dm);
        tm.close();
        dm.close();
    }

    /**
     * 打开一个已有数据库
     */
    private static void openDB(String path, long mem){
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    /**
     * 解析字符串得到对应的memory大小
     */
    private static long parseMem(String memStr){
        if(memStr == null || "".equals(memStr)){
            return DEFAULT_MEM;
        }

        if(memStr.length() < 2){
            Panic.panic(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0,memStr.length()-2));
        switch (unit){
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Panic.panic(Error.InvalidMemException);
        }
        return DEFAULT_MEM;
    }

}
