package com.goodwill.hdr.das.hbase;


import com.goodwill.hdr.das.dto.Page;
import com.goodwill.hdr.das.vo.Condition;

import java.util.List;
import java.util.Map;

public interface HbaseDao {
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
    Map<String, String> selectOne(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns);

    /**
     * 根据条件查询，获取全部记录
     *
     * @param tableName     表名
     * @param patientId     患者ID
     * @param visitId       就诊次
     * @param conditionList 条件列表
     * @param columns       查询字段
     * @return 结果封装
     */
    List<Map<String, String>> selectList(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns);

    /**
     * 根据条件查询，获取分页记录
     *
     * @param tableName     表名
     * @param patientId     患者ID
     * @param visitId       就诊次
     * @param conditionList 条件列表
     * @param columns       查询字段
     * @param pageNo        页码
     * @param pageSize      页的容量
     * @return 结果封装
     */

    Page<Map<String, String>> selectPage(String tableName, String patientId, String visitId, List<Condition> conditionList, List<String> columns, Integer pageNo, Integer pageSize);
}
