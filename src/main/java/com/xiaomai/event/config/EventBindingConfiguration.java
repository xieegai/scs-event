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

import com.xiaomai.event.config.adapter.EventConverterConfigurer;
import com.xiaomai.event.config.adapter.EventHandlerAnnotationBeanPostProcessor;
import com.xiaomai.event.config.adapter.EventHandlerMethodFactory;
import com.xiaomai.event.partition.kafka.KafkaTopicPartitionRefreshJob;
import com.xiaomai.event.lifecycle.IEventLifecycle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.stream.binder.BinderConfiguration;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.BinderType;
import org.springframework.cloud.stream.binder.BinderTypeRegistry;
import org.springframework.cloud.stream.binder.DefaultBinderFactory;
import org.springframework.cloud.stream.binder.DefaultBinderFactory.Listener;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.config.BinderProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Role;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.support.MapArgumentResolver;
import org.springframework.integration.handler.support.PayloadExpressionArgumentResolver;
import org.springframework.integration.handler.support.PayloadsArgumentResolver;
import org.springframework.integration.support.NullAwarePayloadArgumentResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;

import java.util.LinkedList;
import java.util.List;

@Configuration
@EnableConfigurationProperties({EventBindingServiceProperties.class})
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class EventBindingConfiguration {

    public static final String EVENT_HANDLER_ANNOTATION_BEAN_POST_PROCESSOR_NAME =
            "eventHandlerAnnotationBeanPostProcessor";

    @Autowired(required = false)
    private Collection<Listener> binderFactoryListeners;

    @Bean("eventBinderFactory")
    public BinderFactory binderFactory(BinderTypeRegistry binderTypeRegistry,
        EventBindingServiceProperties bindingServiceProperties) {

        DefaultBinderFactory binderFactory = new DefaultBinderFactory(
            getBinderConfigurations(binderTypeRegistry, bindingServiceProperties),
            binderTypeRegistry);
        binderFactory.setDefaultBinder(bindingServiceProperties.getDefaultBinder());
        binderFactory.setListeners(this.binderFactoryListeners);
        return binderFactory;
    }

    @Bean
    public BindingService bindingService(EventBindingServiceProperties eventBindingServiceProperties,
                                         @Qualifier("eventBinderFactory") BinderFactory binderFactory, TaskScheduler taskScheduler) {
        return new BindingService(eventBindingServiceProperties, binderFactory, taskScheduler);
    }

    @Bean
    public EventConverterConfigurer eventConverterConfigurer(EventBindingServiceProperties eventBindingServiceProperties,
        @Qualifier("integrationArgumentResolverMessageConverter") CompositeMessageConverter compositeMessageConverter) {
        return new EventConverterConfigurer(eventBindingServiceProperties, compositeMessageConverter);
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

//    @Bean(IntegrationContextUtils.MESSAGE_HANDLER_FACTORY_BEAN_NAME)
    @Bean(name = "eventHandlerMethodFactory")
    public static MessageHandlerMethodFactory messageHandlerMethodFactory(
        @Qualifier(IntegrationContextUtils.ARGUMENT_RESOLVER_MESSAGE_CONVERTER_BEAN_NAME) CompositeMessageConverter compositeMessageConverter,
        @Nullable Validator validator, ConfigurableListableBeanFactory clbf,
        IEventLifecycle eventLifecycle) {
        EventHandlerMethodFactory messageHandlerMethodFactory = new EventHandlerMethodFactory(eventLifecycle);
        messageHandlerMethodFactory.setMessageConverter(compositeMessageConverter);

        /*
         * We essentially do the same thing as the
         * DefaultMessageHandlerMethodFactory.initArgumentResolvers(..). We can't do it as
         * custom resolvers for two reasons. 1. We would have two duplicate (compatible)
         * resolvers, so they would need to be ordered properly to ensure these new
         * resolvers take precedence. 2.
         * DefaultMessageHandlerMethodFactory.initArgumentResolvers(..) puts
         * MessageMethodArgumentResolver before custom converters thus not allowing an
         * override which kind of proves #1.
         *
         * In all, all this will be obsolete once https://jira.spring.io/browse/SPR-17503
         * is addressed and we can fall back on core resolvers
         */
        List<HandlerMethodArgumentResolver> resolvers = new LinkedList<>();
        resolvers.add(new SmartPayloadArgumentResolver(
          compositeMessageConverter,
          validator));
        resolvers.add(new SmartMessageMethodArgumentResolver(
          compositeMessageConverter));

        resolvers.add(new HeaderMethodArgumentResolver(clbf.getConversionService(), clbf));
        resolvers.add(new HeadersMethodArgumentResolver());

        // Copy the order from Spring Integration for compatibility with SI 5.2
        resolvers.add(new PayloadExpressionArgumentResolver());
        resolvers.add(new NullAwarePayloadArgumentResolver(compositeMessageConverter));
        PayloadExpressionArgumentResolver payloadExpressionArgumentResolver = new PayloadExpressionArgumentResolver();
        payloadExpressionArgumentResolver.setBeanFactory(clbf);
        resolvers.add(payloadExpressionArgumentResolver);
        PayloadsArgumentResolver payloadsArgumentResolver = new PayloadsArgumentResolver();
        payloadsArgumentResolver.setBeanFactory(clbf);
        resolvers.add(payloadsArgumentResolver);
        MapArgumentResolver mapArgumentResolver = new MapArgumentResolver();
        mapArgumentResolver.setBeanFactory(clbf);
        resolvers.add(mapArgumentResolver);

        messageHandlerMethodFactory.setArgumentResolvers(resolvers);
        messageHandlerMethodFactory.setValidator(validator);
        return messageHandlerMethodFactory;
    }

    private static Map<String, BinderConfiguration> getBinderConfigurations(
        BinderTypeRegistry binderTypeRegistry,
        BindingServiceProperties bindingServiceProperties) {

        Map<String, BinderConfiguration> binderConfigurations = new HashMap<>();
        Map<String, BinderProperties> declaredBinders = bindingServiceProperties
            .getBinders();
        boolean defaultCandidatesExist = false;
        Iterator<Entry<String, BinderProperties>> binderPropertiesIterator = declaredBinders
            .entrySet().iterator();
        while (!defaultCandidatesExist && binderPropertiesIterator.hasNext()) {
            defaultCandidatesExist = binderPropertiesIterator.next().getValue()
                .isDefaultCandidate();
        }
        List<String> existingBinderConfigurations = new ArrayList<>();
        for (Map.Entry<String, BinderProperties> binderEntry : declaredBinders
            .entrySet()) {
            BinderProperties binderProperties = binderEntry.getValue();
            if (binderTypeRegistry.get(binderEntry.getKey()) != null) {
                binderConfigurations.put(binderEntry.getKey(),
                    new BinderConfiguration(binderEntry.getKey(),
                        binderProperties.getEnvironment(),
                        binderProperties.isInheritEnvironment(),
                        binderProperties.isDefaultCandidate()));
                existingBinderConfigurations.add(binderEntry.getKey());
            }
            else {
                Assert.hasText(binderProperties.getType(),
                    "No 'type' property present for custom binder "
                        + binderEntry.getKey());
                binderConfigurations.put(binderEntry.getKey(),
                    new BinderConfiguration(binderProperties.getType(),
                        binderProperties.getEnvironment(),
                        binderProperties.isInheritEnvironment(),
                        binderProperties.isDefaultCandidate()));
                existingBinderConfigurations.add(binderEntry.getKey());
            }
        }
        for (Map.Entry<String, BinderConfiguration> configurationEntry : binderConfigurations
            .entrySet()) {
            if (configurationEntry.getValue().isDefaultCandidate()) {
                defaultCandidatesExist = true;
            }
        }
        if (!defaultCandidatesExist) {
            for (Map.Entry<String, BinderType> binderEntry : binderTypeRegistry.getAll()
                .entrySet()) {
                if (!existingBinderConfigurations.contains(binderEntry.getKey())) {
                    binderConfigurations.put(binderEntry.getKey(),
                        new BinderConfiguration(binderEntry.getKey(), new HashMap<>(),
                            true, "integration".equals(binderEntry.getKey()) ? false : true));
                }
            }
        }
        return binderConfigurations;
    }
}
