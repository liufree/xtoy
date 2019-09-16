package org.liufree.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusGroupEntity {

    private Long id;

    private String groupName;

    private String groupCode;

    private Date createDate;
}
