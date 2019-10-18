package org.liufree.entity;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 *
 */
@Data
public class BusReceiverEntity {

    //主键
    private Long id;
    //姓名
    private String name;
    //区域
    private String regionCode;
    //地址
    private String address;
    //地址英文名字
    private String enName;
    //家庭成员
    private int memberFamily;
    //创建时间
    private Date createDate;
    //
    public BusRegionEntity busRegion;
    //
    public List<BusGroupEntity> groupList;

    public BusReceiverEntity(Long id, String name, String regionCode, String address){
        this.id = id;
        this.name = name;
        this.regionCode = regionCode;
        this.address = address;
    }

    public BusReceiverEntity(Long id, String name, String regionCode, String address, String enName, int memberFamily){
        this.id = id;
        this.name = name;
        this.regionCode = regionCode;
        this.address = address;
        this.enName = enName;
        this.memberFamily = memberFamily;
        this.createDate  = new Date();

    }

    public BusReceiverEntity(){

    }

    public String toBuildStr(){
        StringBuffer sb = new StringBuffer();
        sb.append(this.id);
        sb.append("\t");
        sb.append(this.name);
        sb.append("\t");
        sb.append(this.address);
        sb.append("\t");
        sb.append(this.enName);
        sb.append("\t");
        sb.append(this.memberFamily);
        sb.append("\n");
        return sb.toString();
    }

}
