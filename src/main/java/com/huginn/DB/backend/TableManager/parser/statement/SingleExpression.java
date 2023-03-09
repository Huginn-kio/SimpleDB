package com.huginn.DB.backend.TableManager.parser.statement;

/**
 * where语句中支持的条件筛选，目前仅有 filed op value
 */

public class SingleExpression {
    public String field;
    public String compareOp;
    public String value;
}
