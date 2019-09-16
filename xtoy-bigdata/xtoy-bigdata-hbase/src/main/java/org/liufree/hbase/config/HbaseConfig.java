package org.liufree.hbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;

@Configuration
@EnableConfigurationProperties(HbaseProperties.class)
public class HbaseConfig {


    @Bean("conf")
    public org.apache.hadoop.conf.Configuration configuration(@Value("${hbase.zookeeper.quorum}") String quorum,
                                                              @Value("${hbase.zookeeper.port}") String port) {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.port", port);
        return conf;
    }

    @Bean
    public HbaseTemplate hbaseTemplate(org.apache.hadoop.conf.Configuration conf) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        hbaseTemplate.setConfiguration(conf);
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }

    @Bean("hBaseAdmin")
    public Admin createHbaseAdmin(org.apache.hadoop.conf.Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin;
    }


}
