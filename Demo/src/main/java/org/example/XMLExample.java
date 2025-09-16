package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.XMLUtils;

import java.util.Map;

public class XMLExample {
    public static void main(String[] args) {
        try {
            // 1. 初始化 XMLUtils，加载你的 XML 文件
            XMLUtils xmlUtils = new XMLUtils("Demo.xml"); // resources/your_sqls.xml

            // 2. 获取所有 SQL
//            Map<String, String> sqlMap = xmlUtils.getAllSqls();
//            for (Map.Entry<String, String> entry : sqlMap.entrySet()) {
//                System.out.println("SQL ID: " + entry.getKey());
//                System.out.println("SQL: " + entry.getValue());
//                System.out.println("-------------------------------------------------");
//            }
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            tEnv.executeSql(xmlUtils.getSqlById("alerts_ddl"));
            System.out.println("aaaaaaaaaaaaa");
            tEnv.executeSql(xmlUtils.getSqlById("alerts_insert"));

            System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            System.out.println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
