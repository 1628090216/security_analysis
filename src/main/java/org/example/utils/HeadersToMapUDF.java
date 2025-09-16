package org.example.utils;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import java.util.HashMap;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("MAP<STRING, STRING>"))
public class HeadersToMapUDF extends ScalarFunction {

    public Map<String, String> eval(String[] names, String[] values) {
        if (names == null || values == null || names.length != values.length) {
            return new HashMap<>();
        }

        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < names.length; i++) {
            if (names[i] != null && values[i] != null) {
                map.put(names[i].toUpperCase(), values[i]);
            }
        }
        return map;
    }
}