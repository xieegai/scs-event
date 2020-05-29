package com.xiaomai.event.kafka;

import com.xiaomai.event.utils.PartitionRouteUtil;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;

/**
 * @author baihe Created on 2020/5/28 5:05 PM
 */
public class BinderPartitionSelector implements PartitionSelectorStrategy {
    @Override
    public int selectPartition(Object key, int partitionCount) {
        Integer partCount = PartitionRouteUtil.getPartitionCount(key);
        return key.hashCode() % partCount;
    }
}
