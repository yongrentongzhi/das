package com.goodwill.hdr.das_server.hbase;

import com.goodwill.hdr.das_server.vo.Condition;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SpringBootTest
@ExtendWith(SpringExtension.class)
public class HbaseTest {
    @Autowired
    private Connection connection;

    private List<Map<String, String>> scanTable(String tableName,String rowKeyPrefix,List<Condition> conditionList, List<String> columns) {
        Scan scan = new Scan();

        List<Filter> filters = new ArrayList<>();
        if (conditionList != null && !conditionList.isEmpty()) {

            for (Condition condition : conditionList) {
                scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(condition.getFieldName()));
                SingleColumnValueFilter singleColumnValueFilter = Condition.toSingleColumnValueFilter(condition);
                filters.add(singleColumnValueFilter);
            }

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
        }


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




}
