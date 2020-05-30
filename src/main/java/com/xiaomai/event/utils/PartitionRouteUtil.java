/*
 * This file is part of scs-event.
 *
 * scs-event is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * scs-event is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with scs-event.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.xiaomai.event.utils;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author baihe Created on 2020/5/29 7:52 PM
 */
public class PartitionRouteUtil {

    /**
     * The map to store the partition count of binding destination
     */
    private static Map<String, Integer> destinationPartitionCountMap = Maps.newConcurrentMap();

    /**
     * The partition delimiter to extract the destination from the partition key
     */
    private static final String PARTITION_KEY_DELIM = "::";

    /**
     * compose the *FULL* partition key with the event payload, channel, and payload key
     * @param payload the event payload
     * @param channel the sub channel of the event binding
     * @param payloadKey the raw partition key extracted from the payload
     * @return the composed partition key
     */
    public static Object composePartitionKey(Object payload, String channel, Object payloadKey) {
        String destination = EventBindingUtils.resolveDestination(payload.getClass(), channel);
        return destination + PARTITION_KEY_DELIM + payloadKey;
    }

    /**
     * extract the destination of the partition key
     * @param partitionKey the partition key
     * @return the extracted destination
     */
    public static String extractDestination(Object partitionKey) {
        return String.valueOf(partitionKey).split(PARTITION_KEY_DELIM)[0];
    }

    /**
     * Get the partition count with the specified partition key
     * @param partitionKey the partition key
     * @return the partition count
     */
    public static Integer getPartitionCount(Object partitionKey) {
        String destination = extractDestination(partitionKey);
        return destinationPartitionCountMap.getOrDefault(destination, 1);
    }

    /**
     * Update the partition count of the specified destination
     * @param destination the destination
     * @param partCount the partition count
     */
    public static void updateDestinationPartitionCount(String destination, int partCount) {
        destinationPartitionCountMap.putIfAbsent(destination, partCount);
    }


}
