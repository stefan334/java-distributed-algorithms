package com.distributed.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Utils {
    private static final Pattern REGISTER_ID_PATTERN = Pattern.compile("\\[(.*)\\]");

    public static String getRegisterId(String abstractionId) {
        Matcher m = REGISTER_ID_PATTERN.matcher(abstractionId);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    public static String joinHostPort(String host, int port) {
        return host + ":" + port;
    }

}
