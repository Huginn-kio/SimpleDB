package com.huginn.DB.backend.TableManager;

import com.google.common.primitives.Bytes;
import com.huginn.DB.backend.TableManager.parser.statement.*;
import com.huginn.DB.backend.TransactionManager.TransactionManagerImpl;
import com.huginn.DB.backend.utils.Panic;
import com.huginn.DB.backend.utils.ParseStringRes;
import com.huginn.DB.backend.utils.Parser;
import com.huginn.DB.common.Error;

import java.util.*;

/**
 * 一个数据库中存在多张表，TBM使用链表的形式将其组织起来，每一张表都保存一个指向下一张表的UID
 * 表信息也是存放在一个entry中
 * [TableName][NextTable][Field1Uid][Field2Uid]...[FieldNUid]
 */

public class Table {
    TableManager tbm;
    long uid;                                        //本表的uid
    String name;                                     //表名
    byte status;
    long nextUid;                                    //下一张表的uid
    List<Field> fields = new ArrayList<>();          //字段集合

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String name, long nextUid) {
        this.tbm = tbm;
        this.name = name;
        this.nextUid = nextUid;
    }

    /**
     * 根据uid加载一个表信息
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            //get data component of entry
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 事务xid创建一个表对象
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            //检查该字段是否在索引列表index中
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        //持久化表结构信息
        return tb.persistSelf(xid);
    }

    /**
     * 解析数据封装成Table对象
     * [TableName][NextTable][Field1Uid][Field2Uid]...[FieldNUid]
     * TableName是string : [StringLength][StringData]  [4][StringLength]
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        this.name = res.str;
        position += res.next;
        this.nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        //解析字段Uid
        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            //加载field对象并加入
            fields.add(Field.loadField(this, uid));
        }

        return this;
    }

    /**
     * 持久化表结构信息
     * [TableName][NextTable][Field1Uid][Field2Uid]...[FieldNUid]
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }

        this.uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 事务xid执行Delete语句删除记录
     * 返回删除的记录数
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }

        return count;
    }

    /**
     * 事务xid执行Update语句更新数据(只能更新一个字段的值)
     * 返回更新的记录数
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        //查找需要更新的字段
        for (Field field : fields) {
            if (field.fieldName.equals(update.fieldName)) {
                fd = field;
                break;
            }
        }
        //没有找到更新的字段
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }
        //得到更新的值
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (long uid : uids) {
            //得到符合where的其中一条记录
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            //删除旧的记录，但不需要删除索引(IM没有提供删除索引的功能)
            ((TableManagerImpl) tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            entry.put(fd.fieldName, value);
            raw = entry2Raw(entry);
            //将修改后的记录进行插入
            long newUid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
            count++;
            for (Field field : fields) {
                if (field.isIndexed()) {
                    //将更新后的值插入索引中
                    field.insert(entry.get(field.fieldName), newUid);
                }
            }
        }
        return count;
    }

    /**
     * 事务xid执行Select语句查询数据
     */
    public String read(long xid, Select select) throws Exception {
        List<Long> uids = parseWhere(select.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            //得到符合where的其中一条记录
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            //得到装有一条记录的map
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");        //打印一条entry
        }
        return sb.toString();
    }

    /**
     * 事务xid执行insert语句插入数据
     */
    public void insert(long xid, Insert insert) throws Exception {
        //将插入值(string[])变成map<field, value>
        Map<String, Object> entry = string2Entry(insert.values);
        // map -> byte[]
        byte[] raw = entry2Raw(entry);
        //将表示一条记录的byte[]插入并返回该记录所在的uid
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);
        for (Field f : fields) {
            //插入索引
            if (f.isIndexed()){
                //将这个字段的值和这条记录的uid插入到这个字段的索引树中
                f.insert(entry.get(f.fieldName),uid);
            }
        }
    }

    /**
     * 解析Where语句
     * 目前Where语句只能支持带有索引的字段进行筛选
     * 如果Where语句为空，那么根据有索引的字段进行全表查询
     * 返回满足筛选条件的entry的uid
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;
        //根据where语句得到筛选区间
        if (where == null) {
            //无条件筛选
            for (Field field : fields) {
                //寻找有索引的字段
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            //无筛选为全区间
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            //存在where语句
            for (Field field : fields) {
                //where目前仅支持单字段筛选
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotFoundException;
                    }
                    fd = field;
                    break;
                }
            }
            //where语句中的字段不正确
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }
        //得到字段值符合[l0,r0]的entry的uid
        List<Long> uids = fd.search(l0, r0);
        if (!single) {
            //非single, 即两段区间, 需要考虑[l1, r1]
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }


    class CalWhereRes {
        //lr0: 第一个筛选条件的范围区间
        //lr1: 第二个筛选条件的范围区间
        long l0, r0, l1, r1;
        // 是否是单区间, 单区间的话只考虑l0,r0的范围
        boolean single;
    }

    /**
     * 根据where语句筛选出值的一个范围区间
     * where的条件筛选目前只支持单字段进行筛选, 即singleExp1.field = singleExp2.field
     * 例如: id > 1 and id < 4 或者  age > 10 or age < 3
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch (where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) res.l0 = res.l1;
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /**
     * 打印记录
     */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * 解析记录为一个map
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 1;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes r = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将记录转化成byte[]
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    /**
     * 将value数组转化成 map<FieldName, value>
     */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        //记录的值数量和字段不相同
        if (values.length != fields.size()){
            throw Error.InvalidValuesException;
        }

        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName,v);
        }
        return entry;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
