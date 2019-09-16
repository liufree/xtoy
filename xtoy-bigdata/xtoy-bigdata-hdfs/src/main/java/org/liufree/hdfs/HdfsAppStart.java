package org.liufree.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author lwx
 * 9/9/19
 * liufreeo@gmail.com
 */
@SpringBootApplication
public class HdfsAppStart {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(HdfsAppStart.class);
        // app.setWebEnvironment(false);
        app.run(args);
    }
}
