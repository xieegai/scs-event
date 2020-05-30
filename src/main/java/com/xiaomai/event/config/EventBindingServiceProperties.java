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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.xiaomai.event.annotation.EventHandler;
import com.xiaomai.event.annotation.EventMeta;
import com.xiaomai.event.annotation.EventProducer;
import com.xiaomai.event.constant.EventBuiltinAttr;
import com.xiaomai.event.enums.EventBindingType;
import com.xiaomai.event.utils.EventBindingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

@ConfigurationProperties("spring.cloud.stream")
@JsonInclude(Include.NON_DEFAULT)
@Slf4j
public class EventBindingServiceProperties extends BindingServiceProperties {

    @Value("${spring.application.name}")
    private String appName;

    public BindingProperties getBindingProperties(String bindingName) {
        String eventName = EventBindingUtils.resolveEventName(bindingName);
        Class<?> eventPayloadClass = EventBindingUtils.getEventPayloadClass(eventName);
        if (null == eventPayloadClass) {
            log.debug("event not registered: {}, fallback to spring cloud stream message", eventName);
            return super.getBindingProperties(bindingName);
        }

        EventBindingType eventBindingType = EventBindingUtils.resolveEventBindingType(bindingName);
        BindingProperties bindingProperties = new BindingProperties();
        String channel = EventBindingUtils.resolveEventChannel(bindingName);
        String destination = EventBindingUtils.resolveDestination(eventPayloadClass, channel);
        if (this.getBindings().containsKey(bindingName)) {
            BeanUtils.copyProperties(this.getBindings().get(bindingName), bindingProperties);
        }
        if (bindingProperties.getDestination() == null) {
            bindingProperties.setDestination(destination);
        }

        EventProducer eventProducer = EventBindingUtils.getEventProducerConf(eventPayloadClass);
        EventHandler eventHandler = EventBindingUtils.getEventHandlerConf(eventPayloadClass);

        if (eventBindingType == EventBindingType.OUTPUT) {
            if (eventProducer != null && StringUtils.hasText(eventProducer.binder())) {
                bindingProperties.setBinder(eventProducer.binder());
            }
            ProducerProperties producerProperties = bindingProperties.getProducer();
            if (producerProperties == null) {
                producerProperties = new ProducerProperties();
                bindingProperties.setProducer(producerProperties);

                if (null != eventProducer && eventProducer.usePartitionKey()
                    && null == producerProperties.getPartitionKeyExpression()) {
                    SpelExpressionParser parser = new SpelExpressionParser();
                    Expression partitionExpr = parser
                        .parseExpression("headers['" + EventBuiltinAttr.EVENT_KEY.getKey() + "']");
                    producerProperties.setPartitionKeyExpression(partitionExpr);
                }
                if (null != eventProducer && eventProducer.partitions() > 0) {
                    producerProperties.setPartitionCount(eventProducer.partitions());
                } else {
                    producerProperties.setPartitionSelectorName(EventAgentConfiguration.EVENT_BINDER_PARTITION_SELECTOR_NAME);
                }
            }
        } else {
            bindingProperties.setGroup(appName);
            if (eventHandler != null) {
                if (StringUtils.hasText(eventHandler.binder())) {
                    bindingProperties.setBinder(eventHandler.binder());
                }
                if (StringUtils.hasText(eventHandler.group())) {
                    bindingProperties.setGroup(eventHandler.group());
                }
                // initialize the consumer properties
                ConsumerProperties consumerProperties = bindingProperties.getConsumer();
                if (null != consumerProperties) {
                    if (eventHandler.concurrency() > 1) {
                        consumerProperties.setConcurrency(eventHandler.concurrency());
                    }
                    if (eventHandler.maxAttempts() > 1) {
                        consumerProperties.setMaxAttempts(eventHandler.maxAttempts());
                    }
                }
            }
        }

        return bindingProperties;
    }
}
