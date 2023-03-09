package com.huginn.DB.backend.TableManager.parser;

import com.huginn.DB.common.Error;

/**
 * 对语句进行逐字节解析，根据空白符或者上述词法规则，将语句切割成多个 token
 */

public class Tokenizer {
    private byte[] stat;                                  //要解析的statement
    private int pos;                                      //解析的位置
    private String currentToken;                          // 最近的token
    private boolean flushToken;                           // 用于peek token
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 返回最近的一个token
     */

    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 设置flushToken使其可以在peek中返回下一个token
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * 返回一个错误解析的位置信息
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length + 3];
        System.arraycopy(stat, 0, res, 0, pos);    //res: [0 ···· pos]
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);  //res: [0 ··· pos<< ]
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos); //res: [0 ··· pos<< pos+1 ···]
        return res;
    }

    /**
     * 使pos加1
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    /**
     * 返回statement的pos位置的字节
     */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    /**
     * 返回一个解析数据
     */
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 返回一个数据，要么是符号(<) 要么是数字字母下划线的token  要么是引号里的内容
     */
    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte(); //取出一个字节(字符)
            if(b == null) {      //pos超出了stat的长度
                return "";
            }
            if(!isBlank(b)) {    //非blank即退出loop
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {        //是符号
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {               //是双引号或者单引号
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {        //是字母或者数字
            return nextTokenState();
        } else {                                         //超出了解析范围
            err = Error.InvalidCommandException;
            throw err;
        }
    }


    /**
     * 解析遇到字母数字时进入
     * 返回stat中的一个token，由字母数字下划线组成
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {     // 空 or 非字母数字下划线返回
                if(b != null && isBlank(b)) {                                  // 空白
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));                             //加入字母数字下划线
            popByte();
        }
    }

    /**
     * 当解析到 "" ''时进入
     * 返回引用里的内容
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /**
     * 判断b是否是数字
     */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /**
     * 判断b是否是英文字母
     */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * b是否是符号
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')');
    }

    /**
     * b是否是空白格
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
