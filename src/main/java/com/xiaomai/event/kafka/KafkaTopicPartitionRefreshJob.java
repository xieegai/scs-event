package com.xiaomai.event.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
