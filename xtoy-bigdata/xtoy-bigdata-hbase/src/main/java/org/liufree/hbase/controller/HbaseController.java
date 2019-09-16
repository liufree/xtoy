package org.liufree.hbase.controller;


import com.alibaba.fastjson.JSON;
import org.liufree.annotation.CalTime;
import org.liufree.entity.BusReceiverEntity;
import org.liufree.hbase.service.BusReceiverService;
import org.liufree.utils.NameBuildUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: ynz
 * @Date: 2019/1/29/029 10:33
 * @Version 1.0
 */
@RestController
public class HbaseController {

    @Autowired
    private BusReceiverService busReceiverService;

    @RequestMapping("/save")
    public String save(){
        BusReceiverEntity busReceiverEntity = NameBuildUtils.buildReceiver();
        busReceiverService.save(busReceiverEntity);
        return "save";
    }

    @RequestMapping("/batchSave/{count}")
    public String batchSave(@PathVariable int count){
        List<BusReceiverEntity> sList = new ArrayList<BusReceiverEntity>();
        for (int i = 0; i < count; i++) {
            sList.add(NameBuildUtils.buildReceiver());
            if(sList.size()%20000==0
                    || i==count-1 ){
                busReceiverService.batchSave(sList);
                sList.clear();
            }

        }
        return "saveBatch";
    }

    @RequestMapping("/findById/{id}")
    @CalTime
    public String findById(@PathVariable String id){
        return JSON.toJSONString(busReceiverService.queryByRowId(id));
    }

    @RequestMapping("/deleteAll")
    @CalTime
    public String deleteAll(@RequestParam String tableName){
        busReceiverService.deleteAll(tableName);
        return "deleteAll";
    }


}
