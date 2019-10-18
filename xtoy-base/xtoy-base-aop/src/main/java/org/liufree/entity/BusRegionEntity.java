package org.liufree.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusRegionEntity {

    private Long id;

    private String regionName;

    private String regionCode;

 /*   public BusRegionEntity(){

    }

    public BusRegionEntity(Long id,String regionCode,String regionName){
        this.id=id;
        this.regionCode = regionCode;
        this.regionName = regionName;
    }*/

}
