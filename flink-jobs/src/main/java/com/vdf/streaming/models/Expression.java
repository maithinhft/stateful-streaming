package com.vdf.streaming.models;

import java.io.Serializable;
import java.util.List;

/**
 * Biểu thức điều kiện.
 */
public class Expression implements Serializable {
    private String field;
    private String expr;
    private String function;
    private String op;
    private Object value;
    private String rightField;
    private String rightExpr;
    private String orderBy;
    private List<String> keyFields;
    private String ttl;

    public Expression() {}

    public Expression(String field, String expr, String function, String op, Object value, String rightField, String rightExpr, String orderBy, List<String> keyFields, String ttl) {
        this.field = field;
        this.expr = expr;
        this.function = function;
        this.op = op;
        this.value = value;
        this.rightField = rightField;
        this.rightExpr = rightExpr;
        this.orderBy = orderBy;
        this.keyFields = keyFields;
        this.ttl = ttl;
    }

    public String getField() { return field; }
    public void setField(String field) { this.field = field; }

    public String getExpr() { return expr; }
    public void setExpr(String expr) { this.expr = expr; }

    public String getFunction() { return function; }
    public void setFunction(String function) { this.function = function; }

    public String getOp() { return op; }
    public void setOp(String op) { this.op = op; }

    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }

    public String getRightField() { return rightField; }
    public void setRightField(String rightField) { this.rightField = rightField; }

    public String getRightExpr() { return rightExpr; }
    public void setRightExpr(String rightExpr) { this.rightExpr = rightExpr; }

    public String getOrderBy() { return orderBy; }
    public void setOrderBy(String orderBy) { this.orderBy = orderBy; }

    public List<String> getKeyFields() { return keyFields; }
    public void setKeyFields(List<String> keyFields) { this.keyFields = keyFields; }

    public String getTtl() { return ttl; }
    public void setTtl(String ttl) { this.ttl = ttl; }

    @Override
    public String toString() {
        return "Expression{" +
                "field='" + field + '\'' +
                ", expr='" + expr + '\'' +
                ", function='" + function + '\'' +
                ", op='" + op + '\'' +
                ", value=" + value +
                ", rightField='" + rightField + '\'' +
                ", rightExpr='" + rightExpr + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", keyFields=" + keyFields +
                ", ttl='" + ttl + '\'' +
                '}';
    }
}
