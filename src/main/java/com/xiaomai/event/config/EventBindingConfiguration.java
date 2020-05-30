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

package com.xiaomai.event.config;

import com.xiaomai.event.config.adapter.EventHandlerAnnotationBeanPostProcessor;
import com.xiaomai.event.config.adapter.EventHandlerMethodFactory;
import com.xiaomai.event.partition.kafka.KafkaTopicPartitionRefreshJob;
import com.xiaomai.event.lifecycle.IEventLifecycle;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.ContentTypeConfiguration;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.integration.config.HandlerMethodArgumentResolversHolder;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.scheduling.TaskScheduler;

@Configuration
@EnableConfigurationProperties({EventBindingServiceProperties.class})
@Import({ContentTypeConfiguration.class})
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class EventBindingConfiguration {

    public static final String EVENT_HANDLER_ANNOTATION_BEAN_POST_PROCESSOR_NAME =
            "eventHandlerAnnotationBeanPostProcessor";

    @Bean
    public BindingService bindingService(EventBindingServiceProperties eventBindingServiceProperties,
                                         BinderFactory binderFactory, TaskScheduler taskScheduler) {
        return new BindingService(eventBindingServiceProperties, binderFactory, taskScheduler);
    }

    @Bean
    @ConditionalOnClass(AdminClient.class)
    public KafkaTopicPartitionRefreshJob kafkaTopicPartitionRefreshJob(EventBindingServiceProperties eventBindingServiceProperties) {
        return new KafkaTopicPartitionRefreshJob(eventBindingServiceProperties, 0);
    }

    @Bean(name = EVENT_HANDLER_ANNOTATION_BEAN_POST_PROCESSOR_NAME)
    public static EventHandlerAnnotationBeanPostProcessor eventHandlerAnnotationBeanPostProcessor() {
        return new EventHandlerAnnotationBeanPostProcessor();
    }

    @Bean(name = "eventHandlerMethodFactory")
    public static MessageHandlerMethodFactory messageHandlerMethodFactory(CompositeMessageConverterFactory compositeMessageConverterFactory,
                                                                          @Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVERS_BEAN_NAME) HandlerMethodArgumentResolversHolder ahmar,
                                                                          IEventLifecycle eventLifecycle) {
        EventHandlerMethodFactory eventHandlerMethodFactory = new EventHandlerMethodFactory(eventLifecycle);
        eventHandlerMethodFactory.setMessageConverter(compositeMessageConverterFactory.getMessageConverterForAllRegistered());
        eventHandlerMethodFactory.setCustomArgumentResolvers(ahmar.getResolvers());
        return eventHandlerMethodFactory;
    }
}
