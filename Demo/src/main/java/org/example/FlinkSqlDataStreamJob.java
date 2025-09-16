package org.example;

import org.apache.flink.table.api.Table;
import org.example.utils.HeadersToMapUDF;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.io.IOException;

public class FlinkSqlDataStreamJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Kafka 源表
        String kafkaSourceDDL =
            "CREATE TABLE http_raw (" +
            "  id STRING," +
            "  ts BIGINT," +
            "  sid STRING," +
            "  sip STRING," +
            "  sport INT," +
            "  dip STRING," +
            "  dport INT," +
            "  method STRING," +
            "  uri STRING," +
            "  request_header_names ARRAY<STRING>," +
            "  request_header_values ARRAY<STRING>," +
            "  request_body STRING," +
            "  status_code INT," +
            "  response_body STRING" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'http_logs'," +
            "  'properties.bootstrap.servers' = 'kafka:9092'," +
            "  'properties.group.id' = 'flink-sql-group'," +
            "  'format' = 'json'" +
            ")";

        tableEnv.executeSql(kafkaSourceDDL);

        // 2. 注册 UDF
        tableEnv.createTemporaryFunction("headers_to_map", HeadersToMapUDF.class);

        // 3. 使用 SQL 提取关键字段
        String sql =
            "SELECT " +
            "  sip, " +
            "  uri, " +
            "  method, " +
            "  status_code, " +
            "  request_body, " +
            "  headers_to_map(request_header_names, request_header_values)['HOST'] AS host, " +
            "  headers_to_map(request_header_names, request_header_values)['USER-AGENT'] AS user_agent, " +
            "  headers_to_map(request_header_names, request_header_values)['COOKIE'] AS cookie, " +
            "  ts " +
            "FROM http_raw";

        Table enrichedTable = tableEnv.sqlQuery(sql);
        DataStream<HttpLog> enrichedStream = tableEnv.toDataStream(enrichedTable, HttpLog.class);

        // 4. 使用 DataStream API 做安全检测
        enrichedStream
            .keyBy(log -> log.sip)
            .process(new SecurityDetectionFunction())
            .print(); // 输出到控制台，可改为 Kafka

        // 5. 启动
        env.execute("Flink SQL + DataStream Security Monitor");
    }

    // 安全检测逻辑
    public static class SecurityDetectionFunction extends KeyedProcessFunction<String, HttpLog, Alert> {

        private ValueState<Integer> requestCount;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            requestCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("req-count", Integer.class, 0)
            );
        }

        @Override
        public void processElement(HttpLog log, Context ctx, Collector<Alert> out) throws IOException {
            // 更新请求计数
            Integer count = requestCount.value() == null ? 0 : requestCount.value();
            count++;
            requestCount.update(count);

            // 检测 SQL 注入
            if (isSqlInjection(log.uri, log.requestBody)) {
                out.collect(new Alert("SQLi", log.sip, log.uri, log.user_agent));
                log.riskLevel = "high";
            }

            // 高频访问检测
            if (count > 50) { // 示例：超过50次
                out.collect(new Alert("BruteForce", log.sip, "Too many requests", "count=" + count));
            }

            // 注册定时器（5分钟后重置）
            ctx.timerService().registerProcessingTimeTimer(
                ctx.timerService().currentProcessingTime() + 300_000L
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
            requestCount.clear();
        }

        private boolean isSqlInjection(String uri, String body) {
            String content = (uri + " " + body).toLowerCase();
            return content.contains("union select") ||
                   content.contains("sleep(") ||
                   content.contains("' or '1'='1") ||
                   content.matches(".*exec\\(.*");
        }
    }

    // 告警类
    public static class Alert {
        public String type;
        public String ip;
        public String detail;
        public String info;

        public Alert(String type, String ip, String detail, String info) {
            this.type = type;
            this.ip = ip;
            this.detail = detail;
            this.info = info;
        }

        @Override
        public String toString() {
            return "ALERT[" + type + "] " + ip + " -> " + detail + " (" + info + ")";
        }
    }
}