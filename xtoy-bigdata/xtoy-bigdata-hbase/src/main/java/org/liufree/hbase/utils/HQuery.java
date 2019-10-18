package org.liufree.hbase.utils;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;

import java.io.Serializable;
import java.util.List;

@Data
public class HQuery {
    private String table;
    private String family;
    private String qualifier;
    private String qualifierValue;
    private Serializable row;
    private String startRow;
    private String stopRow;
    private Filter filter;
    private PageFilter pageFilter;
    private Scan scan;

    private List<HBaseColumn> columns = Lists.newArrayList();

    public HQuery(){}
    public HQuery(String table){
        this.table = table;
    }

    public HQuery(String table,Serializable row){
        this.table = table;
        this.row = row;
    }

    public HQuery(String table,Serializable row,String family){
        this.table = table;
        this.row = row;
        this.family = family;
    }

}
