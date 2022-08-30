package com.goodwill.hdr.das.vo;

public class Condition {
    /**
     * 数据库字段名
     */
    private String fieldName;
    private String javaType;
    private String value;
    private String matchType;

    public Condition(String propertyName, String propertyType, String propertyValue, String matchType) {
        this.fieldName = propertyName;
        this.javaType = propertyType;
        this.value = propertyValue;
        this.matchType = matchType;
    }

    public Condition() {

    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getJavaType() {
        return javaType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMatchType() {
        return matchType;
    }

    public void setMatchType(String matchType) {
        this.matchType = matchType;
    }
}
