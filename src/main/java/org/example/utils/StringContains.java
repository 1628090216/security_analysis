package org.example.utils;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;

/**
 * 自定义 UDF：判断字符串是否包含指定子串（忽略大小写）
 *
 * SQL 使用示例：
 * SELECT STRING_CONTAINS('Hello World', 'hello');  --> true
 * SELECT STRING_CONTAINS(user_agent, keyword);     --> true/false
 */
@FunctionHint(output = @DataTypeHint("BOOLEAN"))
public class StringContains extends ScalarFunction {

    /**
     * 判断 text 是否包含 keyword（忽略大小写）
     *
     * @param text    被搜索的字符串（如 User-Agent）
     * @param keyword 要查找的关键字（如 "eval"）
     * @return true 如果包含（忽略大小写），否则 false
     */
    public boolean eval(String text, String keyword) {
        // null 安全处理：任意一个为 null 返回 false
        if (text == null || keyword == null) {
            return false;
        }

        // 转小写后判断是否包含
        return text.toLowerCase().contains(keyword.toLowerCase());
    }
}