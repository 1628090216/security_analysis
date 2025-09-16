package org.example.utils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Base64;
public class IsBase64Function extends ScalarFunction {

    @DataTypeHint("BOOLEAN")
    public boolean eval(@DataTypeHint("STRING") String input) {
        if (input == null || input.isEmpty()) {
            return false;
        }

        // Base64 字符集：A-Za-z0-9+/=，且长度 % 4 == 0
        if (!input.matches("^[A-Za-z0-9+/]*={0,2}$")) {
            return false;
        }

        try {
            Base64.getDecoder().decode(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
