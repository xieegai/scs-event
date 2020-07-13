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
package com.xiaomai.event.partition.kafka;

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
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The kafka topic partition refresh job
 * @author baihe
 */
@Slf4j
public class KafkaTopicPartitionRefreshJob implements InitializingBean {

    private static final String TYPE_KAFKA = "kafka";

    private static final String SCS_BROKER_KEY = "spring.cloud.stream.kafka.binder.brokers";

    /**
     * the refresh interval
     * if 0 is set, the refresh routine is disabled
     */
    private final int refreshInterval;

    /**
     * the thread pool scheduled executor to handle the refresh routine
     */
    private final ScheduledExecutorService executorService;

    /**
     * the event binding service properties to retrieve the binder properties
     */
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
     * heartbeat function to refresh the topic partitions
     */
    private synchronized void refreshTopicPartitions() {
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

    /**
     * Get the partition count from the specified topic and binder
     * @param topic the kafka topic
     * @param binder the kafka binder name
     * @return the partition count of the topic
     */
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
                    TopicDescription topicDescription = topicDescriptions.get(topic);
                    partitions = topicDescription.partitions().size();
                } catch (Exception e) {
//                    throw new ProvisioningException("Problems encountered with partitions finding", e);
                    log.error("Problems encountered with partitions finding: ", e);
                    partitions = 1;
                }
                return partitions;
            }
        }
        return 1;
    }

    /**
     * the after properties set hook
     */
    @Override
    public void afterPropertiesSet() {
        // refresh the topic partitions at the beginning
        refreshTopicPartitions();
        // if the refresh heartbeat interval is set, refresh the topic partition periodically
        if (refreshInterval > 0) {
            executorService.scheduleAtFixedRate(this::refreshTopicPartitions, 0,
                refreshInterval, TimeUnit.SECONDS);
        }
    }

    /**
     * Util function to transform the binder
     * @param binderProperties the given binder properties
     * @return the parsed admin client properties
     */
    private static Map<String, Object> parseAdminClientProperties(BinderProperties binderProperties) {
        Object brokers = binderProperties.getEnvironment().get(SCS_BROKER_KEY);
        if (brokers == null) {
            String[] tokens = SCS_BROKER_KEY.split("\\.");
            Map<String, Object> container = binderProperties.getEnvironment();
            int i = 0;
            for (; i < tokens.length - 1; i++) {
                String token = tokens[i];
                if (container.containsKey(token)) {
                    container = (Map<String, Object>) container.get(token);
                }
            }
            if (i == tokens.length - 1) {
                brokers = container.get(tokens[i]);
            }
        }
        return ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    }
}
