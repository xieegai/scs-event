package com.xiaomai.event.utils;

/**
 * @author baihe Created on 2020/5/29 7:52 PM
 */
public class PartitionRouteUtil {

    private static final String PARTITION_KEY_DELIM = "::";

    public static Object composePartitionKey(Object payload, String channel, String payloadKey) {
        String destination = EventBindingUtils.resolveDestination(payload.getClass(), channel);
        return destination + PARTITION_KEY_DELIM + payloadKey;
    }

    public static String extractDestination(Object partitionKey) {
        return String.valueOf(partitionKey).split(PARTITION_KEY_DELIM)[0];
    }

    public static Integer getPartitionCount(Object partitionKey) {
        return 1;
    }

}
