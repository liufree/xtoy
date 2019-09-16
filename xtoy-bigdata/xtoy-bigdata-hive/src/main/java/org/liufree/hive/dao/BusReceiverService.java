package org.liufree.hive.dao;


import org.liufree.entity.BusReceiverEntity;

import java.util.List;

/**
 * @Author: ynz
 * @Date: 2019/1/28/028 17:46
 * @Version 1.0
 */
public interface BusReceiverService {

    void createTable();

    void loadData(String pathFile);

    void insert(BusReceiverEntity busReceiver);

    void deleteAll();

    List<BusReceiverEntity> getAll();
}
