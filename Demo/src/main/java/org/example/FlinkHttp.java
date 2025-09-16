package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.utils.IsBase64Function;
import org.example.utils.StringContains;
import org.example.utils.XMLUtils;

import java.util.Arrays;
import java.util.List;

public class FlinkHttp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 注册自定义函数, 用于判断是否为 Base64
        tEnv.createTemporarySystemFunction("isBase64", IsBase64Function.class);
        tEnv.createTemporarySystemFunction("StringContains", StringContains.class);
        // 1. 加载 XML SQL
        XMLUtils xmlUtils = new XMLUtils("FlinkHttp.xml");
        // 2. 添加要执行的 DDL
        List<String> ddlIds = Arrays.asList("http_log_source", "http_log_view", "create_print_sink","create_temp_rules");
        // 3. 按顺序执行 DDL
        for (String ddlId : ddlIds) {
            String ddl = xmlUtils.getSqlById(ddlId);
            if (ddl != null && !ddl.isEmpty()) {
                tEnv.executeSql(ddl);
            }
        }
//        tEnv.executeSql("SELECT * FROM temp_rules").print();
        // 4. 创建 StatementSet 并添加要执行的 INSERT 语句（用于并行写入多个目标）
        StatementSet set = tEnv.createStatementSet();
        List<String> insertIds = Arrays.asList("insert_print");
        // 5. 添加所有 INSERT 语句（注意：它们将在同一个作业中「并行执行」，不是顺序执行，避免任务阻塞）
        for (String id : insertIds) {
            set.addInsertSql(xmlUtils.getSqlById(id));
        }
        // 6. 提交并执行所有 INSERT（生成一个包含多个 sink 的 Flink 作业）
        set.execute();
    }
}
