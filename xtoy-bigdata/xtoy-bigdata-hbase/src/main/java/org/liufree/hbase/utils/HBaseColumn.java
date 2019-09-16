package org.liufree.hbase.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class HBaseColumn {

    private String family;
    private String qualifier;
    private String value;
    private Serializable rowKey;
}
