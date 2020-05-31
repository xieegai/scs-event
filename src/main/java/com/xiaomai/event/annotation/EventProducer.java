package com.xiaomai.event.annotation;

/**
 * @author baihe
 * Created on 2020-05-17 01:11
 */
public @interface EventProducer {
  /**
   * The event class to produce
   */
  Class<?> event();

  /**
   * The binder to issue the event
   */
  String binder() default "";

  /**
   * The sub channels ot he event ot produce
   * default: empty array - no sub channel
   */
  String[] channels() default {};

  /**
   * Whether to recognize the user defined partition key
   * default: true
   */
  boolean useEventKey() default true;

  /**
   * The partitions count of the partitioned producer
   * default: 0 - use the binder partition data
   * See {@link com.xiaomai.event.partition.BinderPartitionSelector}
   * when the binder is kafka, {@link com.xiaomai.event.partition.kafka.KafkaTopicPartitionRefreshJob}
   * will maintain the kafka native partition data in {@link com.xiaomai.event.utils.PartitionRouteUtil}
   */
  int partitions() default 0;
}
