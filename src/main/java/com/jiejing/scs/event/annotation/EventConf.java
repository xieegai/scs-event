package com.jiejing.scs.event.annotation;

/**
 * @author baihe
 * Created on 2020-05-17 01:11
 */
public @interface EventConf {
  Class<?> event();
  String binder() default "";
  int partitionCount() default 1;
  String[] produceChannels() default {};
}
