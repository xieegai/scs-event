//package com.xiaomai.event.kafka;
//
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.TopicDescription;
//import org.apache.kafka.common.KafkaFuture;
//import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
//import org.springframework.cloud.stream.provisioning.ProvisioningException;
//
//import java.util.Collections;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author baihe Created on 2020/5/28 5:05 PM
// */
//public class KafkaPartitionSelector implements PartitionSelectorStrategy {
//
//    private final String topic;
//
//    private final Map<String, Object> adminClientProperties;
//
//    public KafkaPartitionSelector(String topic, Map<String, Object> adminClientProperties) {
//        this.topic = topic;
//        this.adminClientProperties = adminClientProperties;
//    }
//
//
//    @Override
//    public int selectPartition(Object key, int partitionCount) {
//
//        try (AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
//            int partitions = 0;
//            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
//            KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();
//
//            Map<String, TopicDescription> topicDescriptions = null;
//            try {
//                topicDescriptions = all.get(this.operationTimeout, TimeUnit.SECONDS);
//            }
//            catch (Exception e) {
//                throw new ProvisioningException("Problems encountered with partitions finding", e);
//            }
//            TopicDescription topicDescription = topicDescriptions.get(name);
//            partitions = topicDescription.partitions().size();
//            return key.hashCode() % partitions;
//        }
//    }
//}
