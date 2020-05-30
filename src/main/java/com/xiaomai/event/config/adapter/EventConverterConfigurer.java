package com.xiaomai.event.config.adapter;

import com.xiaomai.event.config.EventBindingServiceProperties;
import com.xiaomai.event.partition.BinderPartitionHandler;
import com.xiaomai.event.utils.EventBindingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
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
                                  CompositeMessageConverterFactory compositeMessageConverterFactory) {
    super(bindingServiceProperties, compositeMessageConverterFactory);
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
          ProducerProperties producerProperties = bindingProperties.getProducer();

          String eventChannel = EventBindingUtils.resolveEventChannel(channelName);
          String destination = EventBindingUtils.resolveDestination(eventPayloadClass, eventChannel);

          PartitioningInterceptor partitioningInterceptor = new PartitioningInterceptor(destination,
            bindingProperties,
            getPartitionKeyExtractorStrategy(producerProperties),
            getPartitionSelectorStrategy(producerProperties));

          messageChannel.addInterceptor(0, partitioningInterceptor);
        }
      }
    }
  }

  /**
   *
   */
  protected final class PartitioningInterceptor extends ChannelInterceptorAdapter {

    private final BindingProperties bindingProperties;

    private final BinderPartitionHandler partitionHandler;

    PartitioningInterceptor(String destination, BindingProperties bindingProperties,
                            PartitionKeyExtractorStrategy partitionKeyExtractorStrategy,
                            PartitionSelectorStrategy partitionSelectorStrategy) {
      this.bindingProperties = bindingProperties;
      this.partitionHandler = new BinderPartitionHandler(
        destination,
        ExpressionUtils.createStandardEvaluationContext(
          EventConverterConfigurer.this.beanFactory),
        this.bindingProperties.getProducer(), partitionKeyExtractorStrategy,
        partitionSelectorStrategy);
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
      if (!message.getHeaders().containsKey(BinderHeaders.PARTITION_OVERRIDE)) {
        int partition = this.partitionHandler.determinePartition(message);
        return EventConverterConfigurer.this.messageBuilderFactory
          .fromMessage(message)
          .setHeader(BinderHeaders.PARTITION_HEADER, partition).build();
      }
      else {
        return EventConverterConfigurer.this.messageBuilderFactory
          .fromMessage(message)
          .setHeader(BinderHeaders.PARTITION_HEADER,
            message.getHeaders()
              .get(BinderHeaders.PARTITION_OVERRIDE))
          .removeHeader(BinderHeaders.PARTITION_OVERRIDE).build();
      }
    }
  }

  @SuppressWarnings("deprecation")
  private PartitionKeyExtractorStrategy getPartitionKeyExtractorStrategy(ProducerProperties producerProperties) {
    PartitionKeyExtractorStrategy partitionKeyExtractor;
    if (producerProperties.getPartitionKeyExtractorClass() != null) {
      log.warn("'partitionKeyExtractorClass' option is deprecated as of v2.0. Please configure partition "
        + "key extractor as a @Bean that implements 'PartitionKeyExtractorStrategy'. Additionally you can "
        + "specify 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' to specify which "
        + "bean to use in the event there are more then one.");
      partitionKeyExtractor = instantiate(producerProperties.getPartitionKeyExtractorClass(), PartitionKeyExtractorStrategy.class);
    }
    else if (StringUtils.hasText(producerProperties.getPartitionKeyExtractorName())) {
      partitionKeyExtractor = this.beanFactory.getBean(producerProperties.getPartitionKeyExtractorName(), PartitionKeyExtractorStrategy.class);
      Assert.notNull(partitionKeyExtractor, "PartitionKeyExtractorStrategy bean with the name '" + producerProperties.getPartitionKeyExtractorName()
        + "' can not be found. Has it been configured (e.g., @Bean)?");
    }
    else {
      Map<String, PartitionKeyExtractorStrategy> extractors = this.beanFactory.getBeansOfType(PartitionKeyExtractorStrategy.class);
      Assert.isTrue(extractors.size() <= 1,
        "Multiple  beans of type 'PartitionKeyExtractorStrategy' found. " + extractors + ". Please "
          + "use 'spring.cloud.stream.bindings.output.producer.partitionKeyExtractorName' property to specify "
          + "the name of the bean to be used.");
      partitionKeyExtractor = CollectionUtils.isEmpty(extractors) ? null : extractors.values().iterator().next();
    }
    return partitionKeyExtractor;
  }

  @SuppressWarnings("deprecation")
  private PartitionSelectorStrategy getPartitionSelectorStrategy(ProducerProperties producerProperties) {
    PartitionSelectorStrategy partitionSelector;
    if (producerProperties.getPartitionSelectorClass() != null) {
      log.warn("'partitionSelectorClass' option is deprecated as of v2.0. Please configure partition "
        + "selector as a @Bean that implements 'PartitionSelectorStrategy'. Additionally you can "
        + "specify 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' to specify which "
        + "bean to use in the event there are more then one.");
      partitionSelector = instantiate(producerProperties.getPartitionSelectorClass(),
        PartitionSelectorStrategy.class);
    }
    else if (StringUtils.hasText(producerProperties.getPartitionSelectorName())) {
      partitionSelector = this.beanFactory.getBean(producerProperties.getPartitionSelectorName(), PartitionSelectorStrategy.class);
      Assert.notNull(partitionSelector,
        "PartitionSelectorStrategy bean with the name '" + producerProperties.getPartitionSelectorName()
          + "' can not be found. Has it been configured (e.g., @Bean)?");
    }
    else {
      Map<String, PartitionSelectorStrategy> selectors = this.beanFactory.getBeansOfType(PartitionSelectorStrategy.class);
      Assert.isTrue(selectors.size() <= 1,
        "Multiple  beans of type 'PartitionSelectorStrategy' found. " + selectors + ". Please "
          + "use 'spring.cloud.stream.bindings.output.producer.partitionSelectorName' property to specify "
          + "the name of the bean to be used.");
      partitionSelector = CollectionUtils.isEmpty(selectors) ? new DefaultPartitionSelector() : selectors.values().iterator().next();
    }
    return partitionSelector;
  }

  /**
   * Default partition strategy; only works on keys with "real" hash codes, such as
   * String. Caller now always applies modulo so no need to do so here.
   */
  private static class DefaultPartitionSelector implements PartitionSelectorStrategy {

    @Override
    public int selectPartition(Object key, int partitionCount) {
      int hashCode = key.hashCode();
      if (hashCode == Integer.MIN_VALUE) {
        hashCode = 0;
      }
      return Math.abs(hashCode);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T instantiate(Class<?> implClass, Class<T> type) {
    try {
      return (T) implClass.newInstance();
    }
    catch (Exception e) {
      throw new BinderException("Failed to instantiate class: " + implClass.getName(), e);
    }
  }
}
