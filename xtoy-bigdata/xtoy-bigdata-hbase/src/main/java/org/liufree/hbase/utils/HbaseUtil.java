package org.liufree.hbase.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;
import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
@Slf4j
public class HbaseUtil {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private Admin hBaseAdmin;

    /**
     * 创建表
     * @param tableName
     * @param familys
     */
    public  void createTable(String tableName,String[] familys) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (hBaseAdmin.tableExists(table)) {
            log.info("table[{}] already exists!",tableName);
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(table);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            hBaseAdmin.createTable(tableDesc);
        }
    }


    /**
     *
     * @param query
     * @return
     */
    public Object execute(HQuery query) {
        if(StringUtils.isBlank(String.valueOf(query.getRow())) || query.getColumns().isEmpty()){
            return null;
        }
        return hbaseTemplate.execute(query.getTable(), new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                byte[] rowkey = String.valueOf(query.getRow()).getBytes();
                Put put = new Put(rowkey);
                for (HBaseColumn col : query.getColumns()) {
                    put.addColumn(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()),
                            Bytes.toBytes(col.getValue()));
                }
                table.put(put);
                return null;
            }
        });
    }

    /**
     * 批量插入
     * @param query
     * @return
     */
    public Object bufferInsert(HQuery query){
        return hbaseTemplate.execute(query.getTable(), new TableCallback<Object>() {
            @SuppressWarnings("deprecation")
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                table.setAutoFlush(false);
                //设置数据缓存区域
                table.setWriteBufferSize(64*1024*1024);
                for(HBaseColumn col:query.getColumns()){
                    byte[] rowkey = String.valueOf(col.getRowKey()).getBytes();
                    Put put = new Put(rowkey);
                    put.addColumn(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getQualifier()),
                            Bytes.toBytes(col.getValue()));
                    table.put(put);
                }
                //刷新缓存区
                table.flushCommits();
                //关闭表连接
                table.close();
                //Thread.sleep(20000);
                return null;
            }
        });
    }


    /**
     * 通过表名和key获取一行数据
     * @param query
     * @param c
     * @param <T>
     * @return
     */
    public <T> T get(HQuery query, Class<T> c) {
        if(StringUtils.isBlank(query.getTable()) || StringUtils.isBlank(String.valueOf(query.getRow()))){
            return null;
        }

        return hbaseTemplate.get(query.getTable(), String.valueOf(query.getRow()), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws IllegalAccessException, InstantiationException {
                List<Cell> ceList = result.listCells();
                T item=c.newInstance();
                JSONObject obj = new JSONObject();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        obj.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }else{
                    return null;
                }
                System.out.println(obj.toJSONString());
                item=JSON.parseObject(obj.toJSONString(), c);
                return item;
            }
        });
    }

    /**
     * 通过表名，开始行键和结束行键获取数据
     *
     * @return
     */
    public <T> List<T> find(HQuery query,Class<T> c) {
        //如果未设置scan,设置scan
       /* if (query.getScan() == null) {

            //起止搜索
            if(StringUtils.isNotBlank(query.getStartRow()) && StringUtils.isNotBlank(query.getStopRow())){
                query.setSearchLimit(query.getStartRow(), query.getStopRow());
            }

            //主要配合pageFilter，指定起始点
            if(StringUtils.isNotBlank(query.getStartRow())){
                query.setScanStartRow(query.getStartRow());
            }

            //列匹配搜索
            if(StringUtils.isNotBlank(query.getFamily()) &&StringUtils.isNotBlank(query.getQualifier())
                    &&StringUtils.isNotBlank(query.getQualifierValue())){
                query.setSearchEqualFilter(query.getFamily(),query.getQualifier(),query.getQualifierValue());
            }

            //分页搜索
            if(query.getPageFilter()!=null){
                query.setFilters(query.getPageFilter());
            }

            if(query.getScan()==null){
                query.setScan(new Scan());
            }
        }*/

        //设置缓存
        query.getScan().setCacheBlocks(false);
        query.getScan().setCaching(2000);

        return hbaseTemplate.find(query.getTable(), query.getScan(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws IllegalAccessException, InstantiationException {

                List<Cell> ceList = result.listCells();
                JSONObject obj = new JSONObject();
                T item =c.newInstance();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength());

                        String quali = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength());
                        if(value.startsWith("[")){
                            obj.put(quali, JSONArray.parseArray(value));
                        }else{
                            obj.put(quali, value);
                        }
                    }
                }
                item =JSON.parseObject(obj.toJSONString(), c);
                return item;
            }

        });
    }

    public void delete(HQuery query){
        hbaseTemplate.delete(query.getTable(), String.valueOf(query.getRow()), query.getFamily());
    }

    /**
     * 删除表
     * @param table
     */
    public void deleteAll(String table){
        TableName tableName = TableName.valueOf(table);
        try {
            //在删除一个表之前，需要首先确保它是 disabled
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
