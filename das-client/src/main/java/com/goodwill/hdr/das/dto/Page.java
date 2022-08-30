package com.goodwill.hdr.das.dto;

import java.util.List;

public class Page<T> {
    private int pageNo;
    private int pageSize;
    private List<T> resultList;
    private int totalCount;


    public Page(int pageNo, int pageSize) {
        this.pageNo = pageNo;
        this.pageSize = pageSize;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<T> getResultList() {
        return resultList;
    }

    public void setResultList(List<T> resultList) {
        this.resultList = resultList;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getTotalPage() {
        int i = totalCount / pageSize;
        int j = totalCount % pageSize;
        return j > 0 ? i + 1 : i;
    }


}
