package com.goodwill.hdr.das_server.enums;

import org.apache.hadoop.hbase.filter.CompareFilter;

public enum HbaseMatchTypeEnum {

    EQ("EQ", CompareFilter.CompareOp.EQUAL),

    GT("GT", CompareFilter.CompareOp.GREATER),

    GE("GE", CompareFilter.CompareOp.GREATER_OR_EQUAL),

    LT("LT", CompareFilter.CompareOp.LESS),

    LE("LE", CompareFilter.CompareOp.LESS_OR_EQUAL),

    NE("NE", CompareFilter.CompareOp.NOT_EQUAL),

    IN("IN", CompareFilter.CompareOp.EQUAL),

    NOTIN("NOTIN", CompareFilter.CompareOp.NOT_EQUAL),
    
    ;

    private final String code;
    private final CompareFilter.CompareOp op;

    HbaseMatchTypeEnum(String code, CompareFilter.CompareOp op) {
        this.code = code;
        this.op = op;
    }

    public String getCode() {
        return code;
    }

    public CompareFilter.CompareOp getOp() {
        return op;
    }

}
