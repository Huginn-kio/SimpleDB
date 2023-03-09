package com.huginn.DB.backend.TableManager;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.IndexManager.BPlusTree;
import com.huginn.DB.backend.TableManager.parser.statement.SingleExpression;
import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.ParseStringRes;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.util.Arrays;
import java.util.List;

/**
 * 字段信息
 * 单个字段信息和表信息都是直接保存在 Entry中
 * [FieldName][TypeName][IndexUid]
 * FieldName : 字段名
 * TypeName : 类型名 —— int32， int64, string类型
 * IndexUid : 索引的uid，指向索引树的根  若该字段无索引,则为0
 * <p>
 * FieldName TypeName 存储的都是字节形式的字符串。这里规定一个字符串的存储方式，以明确其存储边界 —— [StringLength][StringData]
 */

public class Field {
    long uid;                                                 // field信息存放在entry中, entry存放在dataItem中，这是其uid
    private Table tb;                                         // field所在的table
    String fieldName;                                         // 字段名
    String fieldType;                                         // 字段类型
    private long index;                                       // 字段索引
    private BPlusTree bt;                                     // 索引的B+树

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /**
     * 根据uid获得table的字段信息
     * 单个Field字段信息存储在Entry中
     * 先通过uid查找出entry，然后得到entry的data部分，然后解析封装成field对象
     */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;

        try {
            //得到entry的data部分
            raw = ((TableManagerImpl) tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assert raw != null;
        //解析获得field对象
        return new Field(uid, tb).parseSelf(raw);
    }

    /**
     * 根据byte[]解析出field对象 [FieldName][TypeName][IndexUid]
     * [FieldName][TypeName] : [StringLength][StringData]  [4][StringLength]
     */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);   //解析出FieldName数据(str是StringLength长度所代表的字符串，next是TypeName开始解析的地点)
        fieldName = res.str;   //这个是字段名
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length)); //解析出其TypeName数据
        fieldType = res.str;
        position += res.next;
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));    //index是索引树的根节点的uid固定是8字节
        if (index != 0) {  //存在索引
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /**
     * 事务xid创建一个字段
     */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType); //检查字段类型
        Field f = new Field(tb, fieldName, fieldType, 0);
        if (indexed) {   //需要索引
            long index = BPlusTree.create(((TableManagerImpl) tb.tbm).dm);   //创建索引B+树并返回其根节点uid
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl) tb.tbm).dm);   //根据根节点uid获取B+树对象
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);

        return f;
    }

    /**
     * 事务xid持久化一个field并返回其插入的uid
     */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        //
        this.uid = ((TableManagerImpl) tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }


    /**
     * 检查字段类型的合法性
     */
    private static void typeCheck(String fieldType) throws Exception {
        if (!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    /**
     * 判断是否有索引
     */
    public boolean isIndexed(){
        return index != 0;
    }

    /**
     * 为字段值加入索引的B+树
     */
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey,uid);
    }

    /**
     * 范围查找在[left, right]区间内的索引的字段值
     */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left,right);
    }

    //记录了这个字段的值的解析结果和解析这个值所造成的偏移
    class ParseValueRes{
        Object v;
        int shift;
    }

    /**
     * 根据TypeName解析字段的值
     */
    public ParseValueRes parseValue(byte[] raw){
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }


    /**
     * 根据表达式exp求出满足筛选的值区间 FiledCalRes: [left, right]
     */
    public FieldCalRes calExp(SingleExpression exp){
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp){
            case "<" :
                res.left = 0;
                //得到表达式右边的value的uid
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if (res.right > 0){
                    res.right--;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(exp.value);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }

    /**
     * 根据TypeName将字段的value转化成指定的类型
     */
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /**
     * 根据TypeName将字段的值转化成uid
     */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    /**
     * 根据TypeName将字段的值转化成字节数组
     */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    /**
     * 根据TypeName打印字段值
     */
    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }
}

