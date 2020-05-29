package com.xiaomai.event.kafka;

import com.xiaomai.event.utils.PartitionRouteUtil;
import java.util.Map;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;

/**
 * @author baihe Created on 2020/5/28 5:05 PM
 */
public class KafkaPartitionSelector implements PartitionSelectorStrategy {

    private final String topic;

    private final Map<String, Object> adminClientProperties;

    public KafkaPartitionSelector(String topic, Map<String, Object> adminClientProperties) {
        this.topic = topic;
        this.adminClientProperties = adminClientProperties;
    }

    @Override
    public int selectPartition(Object key, int partitionCount) {
        Integer partCount = PartitionRouteUtil.getPartitionCount(key);
        return key.hashCode() % partCount;
    }
}
