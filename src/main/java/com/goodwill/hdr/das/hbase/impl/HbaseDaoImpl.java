package com.goodwill.hdr.das.hbase.impl;


import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.goodwill.hdr.das.hbase.HbaseDao;
import com.goodwill.hdr.ocl.dao.hbase.HbaseDao;
import com.goodwill.hdr.ocl.dao.mysql.OclConfigDao;
import com.goodwill.hdr.ocl.utils.HbaseUtil;
import com.goodwill.hdr.ocl.vo.Condition;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Repository
public class HbaseDaoImpl implements HbaseDao {

    private final Integer startOfPatientId;
    private final Integer endOfPatientId;

    private final String orgId;

    private final Connection connection;
    private final ObjectMapper objectMapper;

    @Autowired
    public HbaseDaoImpl(OclConfigDao oclConfigDao, Connection connection, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        String startOfPatientId = oclConfigDao.getConfigValueByCode("startOfPatientId");
        this.startOfPatientId = Integer.parseInt(startOfPatientId);
        String endOfPatientId = oclConfigDao.getConfigValueByCode("endOfPatientId");
        this.endOfPatientId = Integer.parseInt(endOfPatientId);
        this.orgId = oclConfigDao.getConfigValueByCode("orgId");
        this.connection = connection;
    }

    private String getRowKeyPrefix(String patientId, String visitId) {
        String salt = getSalt(patientId);
        if (StringUtils.isNotBlank(visitId)) {
            return salt + "|" + orgId + "|" + patientId + "|" + visitId;
        }
        return salt + "|" + orgId + "|" + patientId;
    }

    private String getSalt(String patientId) {
        if (patientId.length() <= 4) {
            return "zzzz";
        }

        StringBuilder sb = new StringBuilder(patientId.substring(
                patientId.length() + startOfPatientId, patientId.length() + endOfPatientId));
        String salt = sb.reverse().toString();
        return salt;
    }

    /**
     * 根据条件查询，获取第一行记录
     *
     * @param tableName
     * @param patientId     患者ID
     * @param visitId       就诊次
     * @param conditionList 查询条件
     * @param columns       查询字段
     * @return 结果封装
     */
    @Override
    public Map<String, String> selectOne(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns) {
        List<Map<String, String>> queryResult = selectList(tableName, patientId, visitId, conditionList, columns);
        return queryResult.get(0);
    }


    private List<Map<String, String>> scanTable(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns) {
        Scan scan = new Scan();

        List<Filter> filters = new ArrayList<>();
        if (conditionList != null && !conditionList.isEmpty()) {

            for (Condition condition : conditionList) {
                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(condition.getPropertyName()));
                SingleColumnValueFilter singleColumnValueFilter = HbaseUtil.toSingleColumnValueFilter(condition);
                filters.add(singleColumnValueFilter);
            }

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
        }

        String rowKeyPrefix = getRowKeyPrefix(patientId, visitId);

        byte[] startRow = rowKeyPrefix.getBytes(StandardCharsets.UTF_8);
        byte[] stopRow = (rowKeyPrefix + "}").getBytes(StandardCharsets.UTF_8);
        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);
        if (columns != null && !columns.isEmpty()) {
            for (String column : columns) {
                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column));
            }
        }

        ResultScanner scanner = null;
        Table table = null;
        List<Map<String, String>> queryResult = new ArrayList<>();
        try {

            table = connection.getTable(TableName.valueOf(tableName));
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> noVersionMap = result.getNoVersionMap();
                NavigableMap<byte[], byte[]> map = noVersionMap.get(Bytes.toBytes("cf"));
                Map<String, String> stringMap = byteMapToStringMap(map);
                queryResult.add(stringMap);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
// todo :打印日志
                }
            }
        }


        return queryResult;
    }


    private Map<String, String> byteMapToStringMap(Map<byte[], byte[]> source) {
        Map<String, String> result = new HashMap<>();
        for (byte[] key : source.keySet()) {
            String resultKey = Bytes.toString(key);
            String resultValue = Bytes.toString(source.get(key));
            result.put(resultKey, resultValue);
        }
        return result;
    }

    @PreDestroy
    public void doDestroy() {

        if (connection != null) {

            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

    }

    /**
     * 根据条件查询，获取全部记录
     *
     * @param tableName
     * @param patientId     患者ID
     * @param visitId       就诊次
     * @param conditionList
     * @param columns       查询字段
     * @return 结果封装
     */
    @Override
    public List<Map<String, String>> selectList(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns) {
        List<Map<String, String>> queryResult = scanTable(tableName, patientId, visitId, conditionList, columns);
        return queryResult;
    }

    /**
     * 根据条件查询，获取分页记录
     *
     * @param tableName
     * @param patientId     患者ID
     * @param visitId       就诊次
     * @param conditionList
     * @param columns       查询字段
     * @param pageNo        页码
     * @param pageSize      页的容量
     * @return 结果封装
     */
    @Override
    public Page<Map<String, String>> selectPage(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns, Integer pageNo, Integer pageSize) {
        return null;
    }
}
