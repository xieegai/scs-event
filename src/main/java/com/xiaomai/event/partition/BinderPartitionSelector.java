package com.xiaomai.event.partition;

import com.xiaomai.event.utils.PartitionRouteUtil;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;

/**
 * The *native* partition selector accompany with {@link PartitionRouteUtil}
 * @author baihe Created on 2020/5/28 5:05 PM
 */
public class BinderPartitionSelector implements PartitionSelectorStrategy {

    /**
     * Select the partition with the {@link PartitionRouteUtil}
     * @param key the partition key object
     * @param partitionCount the partition count
     * @return the selected partition index
     */
    @Override
    public int selectPartition(Object key, int partitionCount) {
        Integer partCount = PartitionRouteUtil.getPartitionCount(key);
        return Math.abs(key.hashCode()) % partCount;
    }
}
