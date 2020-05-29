package com.xiaomai.event.kafka;

import com.xiaomai.event.config.EventBindingServiceProperties;
import com.xiaomai.event.utils.EventBindingUtils;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cloud.stream.config.BinderProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.util.StringUtils;

@Slf4j
public class KafkaTopicPartitionRefreshJob implements InitializingBean {

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
    public KafkaTopicPartitionRefreshJob(int refreshInterval) {
        this.refreshInterval = refreshInterval;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * 心跳函数
     */
    private synchronized void refresh() {
    }

    private synchronized void refreshTopicPartition(String topic, String binder) {
        BinderProperties binderProperties = eventBindingServiceProperties.getBinders()
            .get(StringUtils.hasText(binder) ? binder : eventBindingServiceProperties.getDefaultBinder());
        if (binderProperties != null && binderProperties.getEnvironment()) {

        }

        try (AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
            int partitions = 0;
            DescribeTopicsResult describeTopicsResult = adminClient
                .describeTopics(Collections.singletonList(topic));
            KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();

            Map<String, TopicDescription> topicDescriptions = null;
            try {
                topicDescriptions = all.get(this.operationTimeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new ProvisioningException("Problems encountered with partitions finding", e);
            }
            TopicDescription topicDescription = topicDescriptions.get(name);
            partitions = topicDescription.partitions().size();
        }
    }

    /**
     * 初始化心跳
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        executorService.scheduleAtFixedRate(this::refresh, 0,
          refreshInterval, TimeUnit.SECONDS);
    }
}
