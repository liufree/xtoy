package org.liufree.hbase.service;


import org.liufree.entity.BusReceiverEntity;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: ynz
 * @Date: 2019/1/29/029 9:47
 * @Version 1.0
 */
public interface BusReceiverService {

    void save(BusReceiverEntity busReceiverEntity);
    void batchSave(List<BusReceiverEntity> list);
    BusReceiverEntity queryByRowId(Serializable id);
    void deleteAll(String table);
    void createTable();
}
