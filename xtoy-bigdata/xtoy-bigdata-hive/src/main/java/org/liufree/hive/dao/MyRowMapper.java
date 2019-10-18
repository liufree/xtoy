package org.liufree.hive.dao;

import org.liufree.entity.BusReceiverEntity;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class MyRowMapper implements RowMapper<BusReceiverEntity> {

    @Override
    public BusReceiverEntity mapRow(ResultSet resultSet, int i) throws SQLException {
//        获取结果集中的数据
        Long id = Long.valueOf(resultSet.getString("id"));
        String name = resultSet.getString("name");
//        把数据封装成User对象
        String address = resultSet.getString("address");

        BusReceiverEntity busReceiverEntity = new BusReceiverEntity();
        busReceiverEntity.setId(id);
        busReceiverEntity.setName(name);
        busReceiverEntity.setAddress(address);
        return busReceiverEntity;
    }
}