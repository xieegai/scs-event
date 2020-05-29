package com.xiaomai.event.utils;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author baihe Created on 2020/5/29 7:52 PM
 */
public class PartitionRouteUtil {

    private static Map<String, Integer> destinationPartitionCountMap = Maps.newConcurrentMap();

    private static final String PARTITION_KEY_DELIM = "::";

    public static Object composePartitionKey(Object payload, String channel, Object payloadKey) {
        String destination = EventBindingUtils.resolveDestination(payload.getClass(), channel);
        return destination + PARTITION_KEY_DELIM + payloadKey;
    }

    public static String extractDestination(Object partitionKey) {
        return String.valueOf(partitionKey).split(PARTITION_KEY_DELIM)[0];
    }

    public static Integer getPartitionCount(Object partitionKey) {
        String destination = extractDestination(partitionKey);
        return destinationPartitionCountMap.getOrDefault(destination, 1);
    }

    public static void updateDestinationPartitionCount(String destination, int partCount) {
        destinationPartitionCountMap.putIfAbsent(destination, partCount);
    }


}
