package org.liufree.hdfs.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author lwx
 * 9/9/19
 * liufreeo@gmail.com
 */
@Slf4j
@Component
public class HadoopConfig {

    @Value("${hadoop.name-node}")
    private String nameNode;

    /**
     * Configuration conf=new Configuration（）；
     * 创建一个Configuration对象时，其构造方法会默认加载hadoop中的两个配置文件，
     * 分别是hdfs-site.xml以及core-site.xml，这两个文件中会有访问hdfs所需的参数值，
     * 主要是fs.default.name，指定了hdfs的地址，有了这个地址客户端就可以通过这个地址访问hdfs了。
     * 即可理解为configuration就是hadoop中的配置信息。
     *
     * @return
     */
    /**
     * 本地文件系统
     *
     * @return
     */
    @Bean("fileSystem")
    public FileSystem createFs() {
        //读取配置文件
        Configuration conf = new Configuration();

        //conf.set("fs.defalutFS", "hdfs://192.168.169.128:9000");
        /*conf.set("dfs.replication", "2");

        conf.set("fs.defaultFS", nameNode);*/
        //指定访问hdfs的客户端身份
        //fs = FileSystem.get(new URI("hdfs://192.168.169.128:9000/"), conf, "root");
        // 文件系统
        FileSystem fs = null;
        // 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统


        try {
            URI uri = new URI(nameNode.trim());
            fs = FileSystem.get(uri, conf);
        } catch (Exception e) {
            log.error("", e);
        }
        System.out.println("fs.defaultFS: " + conf.get("fs.defaultFS"));
        return fs;
    }

    @Bean("distributedFileSystem")
    public DistributedFileSystem createSys() {
        Configuration conf = new Configuration();
        DistributedFileSystem system = null;
        URI uri = null;
        try {
            uri = new URI(nameNode);
            system = (DistributedFileSystem) DistributedFileSystem.get(uri, conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return system;
    }
}
