package org.liufree.hive.controller;

import org.liufree.annotation.CalTime;
import org.liufree.entity.BusReceiverEntity;
import org.liufree.hdfs.config.HadoopTemplate;
import org.liufree.hive.dao.BusReceiverService;
import org.liufree.utils.NameBuildUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

/**
 * @Author: ynz
 * @Date: 2019/1/28/028 17:53
 * @Version 1.0
 */
@RestController
public class HiveController {

    @Autowired
    BusReceiverService busReceiverService;

    @Autowired
    private HadoopTemplate hadoopTemplate;

    @RequestMapping("/create")
    @CalTime
    public String create(){
        busReceiverService.createTable();
        return "create";
    }


    @RequestMapping("/save/{count}")
    @CalTime
    public String save(@PathVariable int count){
        BusReceiverEntity busReceiverEntity = NameBuildUtils.buildReceiver();
        busReceiverService.insert(busReceiverEntity);
        return "save";
    }

    @RequestMapping("/load")
    @CalTime
    public String load(@RequestParam String path){
        //
        busReceiverService.loadData(path);
        return "load";
    }

    @RequestMapping("/load/{count}")
    @CalTime
    public String load(@PathVariable int count){
        String filePath = buildFile( count);
        hadoopTemplate.uploadFile(true,filePath);
        busReceiverService.loadData(hadoopTemplate.getNameSpace()+"/name.txt");
        return "load";
    }

    @RequestMapping("/delAll")
    @CalTime
     public String delAll(){
         busReceiverService.deleteAll();
         return "delAll";
     }

     @RequestMapping("/getAll")
     public List<BusReceiverEntity> getAll() {
        return busReceiverService.getAll();
     }


    public String buildFile(int count){
        File file = new File("name.txt");
        try {

            if(!file.exists()){
                file.createNewFile();
            }
            System.out.println(file.getAbsoluteFile());
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            for(int i = 0; i<count;i++){
                BusReceiverEntity busReceiverEntity = NameBuildUtils.buildReceiver();
                bw.write(busReceiverEntity.toBuildStr());
            }
            bw.close();
        }catch (Exception e){

        }
        return file.getAbsolutePath();
    }
}
