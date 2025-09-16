package org.example;

public class HttpLog {
    public String id;
    public Long ts;
    public String sid;
    public String sip;
    public Integer sport;
    public String dip;
    public Integer dport;
    public String method;
    public String uri;
    public String requestBody;
    public Integer statusCode;
    public String responseBody;

    // 以下字段由 SQL 提取
    public String host;
    public String user_agent;
    public String cookie;

    // 用于告警
    public String riskLevel;

    // 无参构造函数（Flink 反序列化需要）
    public HttpLog() {}

    @Override
    public String toString() {
        return "HttpLog{" +
                "sip='" + sip + '\'' +
                ", uri='" + uri + '\'' +
                ", user_agent='" + user_agent + '\'' +
                ", riskLevel='" + riskLevel + '\'' +
                '}';
    }
}