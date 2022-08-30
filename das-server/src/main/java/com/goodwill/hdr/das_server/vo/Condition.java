package com.goodwill.hdr.das_server.vo;

import com.goodwill.hdr.das_server.enums.HbaseMatchTypeEnum;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

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

    public static SingleColumnValueFilter toSingleColumnValueFilter(Condition condition) {
        String matchType = condition.getMatchType();
        SingleColumnValueFilter singleColumnValueFilter = null;
        for (HbaseMatchTypeEnum matchTypeEnum : HbaseMatchTypeEnum.values()) {
            if (matchType.equals(matchTypeEnum.getCode())) {

                if (matchType.equals(HbaseMatchTypeEnum.IN.getCode()) || matchType.equals(HbaseMatchTypeEnum.NOTIN.getCode())) {
                    String[] values = condition.getValue().split(",");
                    StringBuilder regx = new StringBuilder();
                    for (int i = 0; i < values.length; i++) {
                        if (i > 0) {
                            regx.append("|");
                        }
                        regx.append("(^").append(values[i]).append("$)");
                    }
                    singleColumnValueFilter = new SingleColumnValueFilter(
                            Bytes.toBytes("cf"),
                            Bytes.toBytes(condition.getFieldName()),
                            matchTypeEnum.getOp(),
                            new RegexStringComparator(regx.toString()));

                } else {
                    singleColumnValueFilter = new SingleColumnValueFilter(
                            Bytes.toBytes("cf"),
                            Bytes.toBytes(condition.getFieldName()),
                            matchTypeEnum.getOp(),
                            Bytes.toBytes(condition.getValue())
                    );

                }

            }
        }
        if (singleColumnValueFilter != null) {
            singleColumnValueFilter.setFilterIfMissing(true);
        }
        return singleColumnValueFilter;
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
