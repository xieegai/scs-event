package com.xiaomai.event.config.adapter;

import com.xiaomai.event.config.EventBindingServiceProperties;
import com.xiaomai.event.partition.BinderPartitionHandler;
import com.xiaomai.event.partition.EventPartitionHandler;
import com.xiaomai.event.utils.EventBindingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.cloud.stream.binder.*;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author baihe
 * Created on 2020-05-31 01:06
 */
@Slf4j
public class EventConverterConfigurer extends MessageConverterConfigurer {
  private ConfigurableListableBeanFactory beanFactory;

  private EventBindingServiceProperties eventBindingServiceProperties;

  private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

  public EventConverterConfigurer(EventBindingServiceProperties bindingServiceProperties,
     CompositeMessageConverter compositeMessageConverter) {
    super(bindingServiceProperties, compositeMessageConverter);
    this.eventBindingServiceProperties = bindingServiceProperties;
  }

  @Override
  public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
    super.setBeanFactory(beanFactory);
    this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
  }

  @Override
  public void configureOutputChannel(MessageChannel channel,
                                     String channelName) {
    super.configureOutputChannel(channel, channelName);
    String eventName = EventBindingUtils.resolveEventName(channelName);
    Class<?> eventPayloadClass = EventBindingUtils.getEventPayloadClass(eventName);
    if (null != eventPayloadClass) {
      AbstractMessageChannel messageChannel = (AbstractMessageChannel) channel;
      List<ChannelInterceptor> channelInterceptors = messageChannel.getChannelInterceptors();
      if (!CollectionUtils.isEmpty(channelInterceptors)) {
        ChannelInterceptor toReplaceInterceptor = null;
        for (ChannelInterceptor checkInterceptor: channelInterceptors) {
          if (checkInterceptor.getClass().getName().contains("PartitioningInterceptor")) {
            toReplaceInterceptor = checkInterceptor;
            break;
          }
        }
        if (toReplaceInterceptor != null) {
          messageChannel.removeInterceptor(toReplaceInterceptor);

          BindingProperties bindingProperties = this.eventBindingServiceProperties.getBindingProperties(channelName);

          String eventChannel = EventBindingUtils.resolveEventChannel(channelName);
          String destination = EventBindingUtils.resolveDestination(eventPayloadClass, eventChannel);

          PartitioningInterceptor partitioningInterceptor = new PartitioningInterceptor(destination,
            bindingProperties);
          messageChannel.addInterceptor(0, partitioningInterceptor);
        }
      }
    }
  }

  public final class PartitioningInterceptor implements ChannelInterceptor {
    private final BindingProperties bindingProperties;
    private final EventPartitionHandler partitionHandler;

    PartitioningInterceptor(String destination, BindingProperties bindingProperties) {
      this.bindingProperties = bindingProperties;
      this.partitionHandler =
          new EventPartitionHandler(destination, ExpressionUtils.createStandardEvaluationContext(
              EventConverterConfigurer.this.beanFactory),
              this.bindingProperties.getProducer(), EventConverterConfigurer.this.beanFactory);
    }

    public void setPartitionCount(int partitionCount) {
      this.partitionHandler.setPartitionCount(partitionCount);
    }

    public Message<?> preSend(Message<?> message, MessageChannel channel) {
      if (!message.getHeaders().containsKey("scst_partitionOverride")) {
        int partition = this.partitionHandler.determinePartition(message);
        return EventConverterConfigurer.this.messageBuilderFactory.fromMessage(message).setHeader("scst_partition", partition).build();
      } else {
        return EventConverterConfigurer.this.messageBuilderFactory.fromMessage(message).setHeader("scst_partition", message.getHeaders().get("scst_partitionOverride")).removeHeader("scst_partitionOverride").build();
      }
    }
  }

}
