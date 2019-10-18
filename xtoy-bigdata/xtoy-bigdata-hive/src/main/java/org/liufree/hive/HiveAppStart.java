package org.liufree.hive;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author lwx
 * 9/9/19
 * liufreeo@gmail.com
 */
@SpringBootApplication(scanBasePackages = {"org.liufree"})
public class HiveAppStart {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(HiveAppStart.class);
        // app.setWebEnvironment(false);
        app.run(args);
    }
}
