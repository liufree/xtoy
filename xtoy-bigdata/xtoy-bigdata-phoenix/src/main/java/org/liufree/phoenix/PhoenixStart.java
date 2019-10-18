package org.liufree.phoenix;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.*;

/**
 * @author lwx
 * 9/9/19
 * liufreeo@gmail.com
 * 需要先启动 ./queryserver.py
 */
@SpringBootApplication
public class PhoenixStart {

    public static void main(String[] args) throws SQLException {
        SpringApplication app = new SpringApplication(PhoenixStart.class);
        // app.setWebEnvironment(false);
        app.run(args);

        try {
            // 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
            // Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");

            // phoenix4.3用下面的驱动对应hbase0.98+
            Class.forName("org.apache.phoenix.queryserver.client.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
        String url = "jdbc:phoenix:thin:url=http://host:8765;serialization=PROTOBUF;authentication=SPNEGO;";
        // String url =
        // "jdbc:phoenix:41.byzoro.com,42.byzoro.com,43.byzoro.com:2181";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();
        String sql = "select count(1) as num from SYStEM.CATALOG";
        long time = System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            int count = rs.getInt("num");
            System.out.println("row count is " + count);
        }
        long timeUsed = System.currentTimeMillis() - time;
        System.out.println("time " + timeUsed + "mm");
        // 关闭连接
        rs.close();
        statement.close();
        conn.close();
    }
}
