package com.distributed.utils;

import amcds.pb.AmcdsProto.ProcessId;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common utility functions for distributed abstractions.
 */
public class Utils {
    private static final Pattern REGISTER_ID_PATTERN = Pattern.compile("\\[(.*)\\]");

    /**
     * Extracts the key inside brackets of an abstraction ID, e.g. "app.nnar[x]" → "x".
     */
    public static String getRegisterId(String abstractionId) {
        Matcher m = REGISTER_ID_PATTERN.matcher(abstractionId);
        if (m.find()) {
            return m.group(1);
        } else {
            throw new IllegalArgumentException("Invalid abstractionId: " + abstractionId);
        }
    }

    /**
     * Converts an int to its decimal string representation.
     */
    public static String int32ToString(int i) {
        return Integer.toString(i);
    }

    /**
     * Joins host and port into "host:port".
     */
    public static String joinHostPort(String host, int port) {
        return host + ":" + port;
    }

    /**
     * Builds a unique key for a ProcessId: owner + index.
     */
    public static String getProcessKey(ProcessId p) {
        return p.getOwner() + int32ToString(p.getIndex());
    }

    /**
     * Returns a comparator that orders ProcessIds by rank ascending (max last).
     * For max-rank first, reverse the comparator when using.
     */
    public static Comparator<ProcessId> byRank() {
        return Comparator.comparingInt(ProcessId::getRank);
    }
}
