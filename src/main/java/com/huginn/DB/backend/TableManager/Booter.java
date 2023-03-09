package com.huginn.DB.backend.TableManager;

import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.common.Error;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * 管理 DB 的启动信息
 * 由于 TBM 的表管理，使用的是链表串起的 Table 结构，所以就必须保存一个链表的头节点，即第一个表的 UID，这样在 MYDB 启动时，才能快速找到表信息
 * DB 使用 Booter 类和 bt 文件，来管理 MYDB 的启动信息，虽然现在所需的启动信息，只有一个：头表的 UID
 */

public class Booter {

    public static final String BOOTER_SUFFIX = ".bt";                        //存放启动信息的文件
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";                //修改时存放信息的临时文件

    String path;                                                             //存放启动信息的文件路径
    File file;


    public Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    /**
     * 创建一个启动类及其对应的文件
     */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 根据一个已存在的启动文件创建Booter类
     */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /**
     * 删除废弃的临时文件
     */
    private static void removeBadTmp(String path) {
        new File(path + BOOTER_TMP_SUFFIX).delete();
    }

    /**
     * 读取本启动文件的所有数据
     */
    public byte[] load(){
       byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /**
     * 修改启动文件的信息
     * update 在修改 bt 文件内容时，没有直接对 bt 文件进行修改，而是首先将内容写入一个 bt_tmp 文件中，随后将这个文件重命名为 bt 文件。
     * 以期通过操作系统重命名文件的原子性，来保证操作的原子性。
     */
    public void update(byte[] data){
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        //修改是通过写入一个临时文件，然后通过修改文件名来完成修改
        try(FileOutputStream out = new FileOutputStream(tmp)){
            out.write(data);
            out.flush();
        }catch (IOException e){
            Panic.panic(e);
        }

        try{
            //对原启动文件进行覆盖
            Files.move(tmp.toPath(), new File(path + BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        }catch (IOException e){
            Panic.panic(e);
        }

        //重新进行file的赋值
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }
}
