package com.xiaomai.event.partition;

import com.xiaomai.event.constant.EventBuiltinAttr;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.messaging.Message;

/**
 * @author baihe Created on 2020/5/30 1:08 PM
 */
public class HeaderPartitionKeyExtractor implements PartitionKeyExtractorStrategy {

    @Override
    public Object extractKey(Message<?> message) {
        Object partitionKey = message.getHeaders().get(EventBuiltinAttr.EVENT_KEY.getKey());
        return partitionKey;
    }
}
