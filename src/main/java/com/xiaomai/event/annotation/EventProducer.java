package com.xiaomai.event.annotation;

/**
 * @author baihe
 * Created on 2020-05-17 01:11
 */
public @interface EventProducer {
  Class<?> event();
  String binder() default "";
  String[] channels() default {};
  boolean usePartitionKey() default false;
  int partitions() default 1;
}
