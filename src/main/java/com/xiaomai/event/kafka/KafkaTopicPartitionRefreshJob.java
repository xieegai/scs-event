package com.xiaomai.event.kafka;

import com.google.common.collect.ImmutableMap;
import com.xiaomai.event.config.EventBindingServiceProperties;
import com.xiaomai.event.utils.EventBindingUtils;
import com.xiaomai.event.utils.PartitionRouteUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.config.BinderProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaTopicPartitionRefreshJob implements InitializingBean {

    private static final String TYPE_KAFKA = "kafka";

    private static final String SCS_BROKER_KEY = "spring.cloud.stream.kafka.binder.brokers";

    /**
     * 心跳时长（秒）
     */
    private final int refreshInterval;

    /**
     * 心跳任务的线程池
     */
    private final ScheduledExecutorService executorService;


    private final EventBindingServiceProperties eventBindingServiceProperties;


    /**
     * CONSTRUCTOR
     */
    public KafkaTopicPartitionRefreshJob(EventBindingServiceProperties eventBindingServiceProperties, int refreshInterval) {
        this.eventBindingServiceProperties = eventBindingServiceProperties;
        this.refreshInterval = refreshInterval;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * 心跳函数
     */
    private synchronized void refresh() {
        EventBindingUtils.getEventProducerConfMap().forEach((eventPayloadClass, pconf) -> {
          if (pconf.channels().length > 0) {
              for (String channel: pconf.channels()) {
                  String destination = EventBindingUtils.resolveDestination(eventPayloadClass, channel);
                  int channelPartCount = getTopicPartition(destination, pconf.binder());
                  PartitionRouteUtil.updateDestinationPartitionCount(destination, channelPartCount);
              }
          } else {
              String destination = EventBindingUtils.resolveDestination(eventPayloadClass, null);
              int partCount = getTopicPartition(destination, pconf.binder());
              PartitionRouteUtil.updateDestinationPartitionCount(destination, partCount);
          }
        });
    }

    private int getTopicPartition(String topic, String binder) {
        BinderProperties binderProperties = eventBindingServiceProperties.getBinders()
            .get(StringUtils.hasText(binder) ? binder : eventBindingServiceProperties.getDefaultBinder());
        if (binderProperties != null && binderProperties.getType().equalsIgnoreCase(TYPE_KAFKA)) {
            try (AdminClient adminClient = AdminClient.create(parseAdminClientProperties(binderProperties))) {
                int partitions = 0;
                DescribeTopicsResult describeTopicsResult = adminClient
                  .describeTopics(Collections.singletonList(topic));
                KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();

                Map<String, TopicDescription> topicDescriptions = null;
                try {
                    topicDescriptions = all.get(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new ProvisioningException("Problems encountered with partitions finding", e);
                }
                TopicDescription topicDescription = topicDescriptions.get(topic);
                partitions = topicDescription.partitions().size();
                return partitions;
            }
        }

        return 1;

    }

    /**
     * 初始化心跳
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        refresh();
//        executorService.scheduleAtFixedRate(this::refresh, 0,
//          refreshInterval, TimeUnit.SECONDS);
    }

    private static Map<String, Object> parseAdminClientProperties(BinderProperties binderProperties) {
        Object brokers = binderProperties.getEnvironment().get(SCS_BROKER_KEY);
        return ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    }
}
