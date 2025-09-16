package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.XMLUtils;

import java.util.Arrays;
import java.util.List;

public class FlinkSQLJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 1. 加载 XML SQL
        XMLUtils xmlUtils = new XMLUtils("Demo.xml");

        // 2. 添加要执行的 DDL
        List<String> ddlIds = Arrays.asList("kafka_source", "kafka_source_view", "sink_table", "rules_ddl", "alerts_ddl");
        // 3. 按顺序执行 DDL
        for (String ddlId : ddlIds) {
            String ddl = xmlUtils.getSqlById(ddlId);
            if (ddl != null && !ddl.isEmpty()) {
                tEnv.executeSql(ddl);
            }
        }
        // 4. 创建 StatementSet 并添加要执行的 INSERT 语句（用于并行写入多个目标）
        StatementSet statementSet = tEnv.createStatementSet();
        List<String> insertIds = Arrays.asList("insert_starrocks_table", "dynamic_high_risk_alert_sink");
        // 5. 添加所有 INSERT 语句（注意：它们将在同一个作业中「并行执行」，不是顺序执行，避免任务阻塞）
        for (String id : insertIds) {
            statementSet.addInsertSql(xmlUtils.getSqlById(id));
        }
        // 6. 提交并执行所有 INSERT（生成一个包含多个 sink 的 Flink 作业）
        statementSet.execute();
    }

//    private static void executeIfNotEmpty(StreamTableEnvironment tEnv, String sql) {
//        if (sql != null && !sql.isEmpty()) {
//            tEnv.executeSql(sql);
//        }
//    }
}
