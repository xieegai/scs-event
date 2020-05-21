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
import com.xiaomai.event.constant.EventBuiltinAttr;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import com.xiaomai.event.annotation.EventConf;
import com.xiaomai.event.utils.EventBindingUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

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
            log.warn("event not registered: {}, fallback to spring cloud stream message", eventName);
            return super.getBindingProperties(bindingName);
        }

        BindingProperties bindingProperties = new BindingProperties();
        String channel = EventBindingUtils.resolveEventChannel(bindingName);
        // Destination is named as the channel bean
        String destination = EventBindingUtils.composeEventChannelBeanName(eventName, channel);
        if (this.getBindings().containsKey(bindingName)) {
            BeanUtils.copyProperties(this.getBindings().get(bindingName), bindingProperties);
        }
        if (bindingProperties.getDestination() == null) {
            bindingProperties.setDestination(destination);
        }



        EventConf eventConf = EventBindingUtils.getEventConf(eventPayloadClass);
        if (eventConf != null) {
            if (StringUtils.isNotBlank(eventConf.binder())) {
                bindingProperties.setBinder(eventConf.binder());
            }
        }

        EventHandler eventHandler = EventBindingUtils.getEventConsumerConf(eventPayloadClass);

        // Customize the consumer group
        if (eventHandler != null && StringUtils.isNotBlank(eventHandler.group())) {
            bindingProperties.setGroup(eventHandler.group());
        } else {
            bindingProperties.setGroup(appName);
        }


        if (eventHandler != null) {
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

        // initialize the producer properties
        ProducerProperties producerProperties = bindingProperties.getProducer();
        if (null == producerProperties) {
            producerProperties = new ProducerProperties();
            bindingProperties.setProducer(producerProperties);
        }

        if (null == producerProperties.getPartitionKeyExpression()) {
            SpelExpressionParser parser = new SpelExpressionParser();
            Expression partitionExpr = parser
              .parseExpression("headers['" + EventBuiltinAttr.EVENT_KEY.getKey() + "']");
            producerProperties.setPartitionKeyExpression(partitionExpr);

            if (eventConf != null && eventConf.partitionCount() > 1) {
                producerProperties.setPartitionCount(eventConf.partitionCount());
            }
        }

        return bindingProperties;
    }
}
