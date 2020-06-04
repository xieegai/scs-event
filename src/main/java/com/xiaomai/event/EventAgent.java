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

package com.xiaomai.event;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.xiaomai.event.annotation.EventProducer;
import com.xiaomai.event.annotation.EventMeta;
import com.xiaomai.event.constant.EventBuiltinAttr;
import com.xiaomai.event.lifecycle.IEventLifecycle;
import com.xiaomai.event.utils.EventBindingUtils;
import com.xiaomai.event.utils.PartitionRouteUtil;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author baihe Created on 2020/3/18 1:51 PM
 */
public class EventAgent<T> {

    private static final String DELIM_PARTITION_KEY = "-";

    private final Class<T> payloadClass;

    private final EventMeta eventMeta;

    private final String channel;

    private final String appName;

    private final IEventLifecycle eventLifecycle;

    private final BinderAwareChannelResolver resolver;

    private static final Map<String, EventAgent> agentMap = new ConcurrentHashMap<>();

    private final List<Field> partitionFields;

    /**
     * The internal CONSTRUCTOR of event agent
     * @param payloadClass the event payload class
     * @param channel the event channel
     * @param appName the application name
     * @param eventLifecycle the event life cycle proxy
     * @param resolver the spring-cloud-stream message channel resolver
     */
    protected EventAgent(Class<T> payloadClass, String channel, String appName, IEventLifecycle eventLifecycle, BinderAwareChannelResolver resolver) {
        this.appName = appName;
        this.eventLifecycle = eventLifecycle;
        this.resolver = resolver;
        this.payloadClass = payloadClass;
        this.channel = channel;
        this.eventMeta = payloadClass.getDeclaredAnnotation(EventMeta.class);
        Assert.state(null != this.eventMeta, "the specified payloadClass is not marked with the meta annotation");

        if (this.eventMeta.partitionOn().length > 0) {
            Map<String, Field> fieldMap = Stream.of(this.payloadClass.getDeclaredFields()).collect(Collectors.toMap(
                Field::getName,
                Function.identity()));
            this.partitionFields = Stream.of(this.eventMeta.partitionOn()).map(fname -> {
                Field field = fieldMap.get(fname);
                ReflectionUtils.makeAccessible(field);
                return field;
            }).collect(Collectors.toList());
        } else {
            this.partitionFields = Lists.newArrayList();
        }
    }

    /**
     * Trigger the event with payload
     * @param payload the event payload
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload) {
        return triggerEvent(payload, null);
    }

    /**
     * Trigger the event with payload and partition key
     * @param payload the event payload
     * @param payloadKey the event partition key
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload, Object payloadKey) {
        return triggerEvent(payload, appName, new HashMap<>(), payloadKey);
    }

    /**
     * Trigger the event with payload, producer and partition key
     * @param payload the event payload
     * @param producer the event producer key
     * @param payloadKey the event partition key
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload, String producer, Object payloadKey) {
        return triggerEvent(payload, producer, new HashMap<>(), payloadKey);
    }

    /**
     * Trigger the event with payload and attributes
     * @param payload the event payload
     * @param producer the event producer key
     * @param eventAttrs the event attributes
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload, String producer, Map<String, Object> eventAttrs, Object payloadKey) {
        String eventSeq = eventLifecycle.onIssue(payload, producer, eventAttrs);

        if (null == payloadKey) {
            payloadKey = (!CollectionUtils.isEmpty(partitionFields)) ?
                partitionFields.stream().map(f -> ReflectionUtils.getField(f, payload)).filter(
                    Objects::nonNull).map(String::valueOf)
                    .collect(Collectors.joining(DELIM_PARTITION_KEY))
                : Math.abs(payload.hashCode());
        }
        // compose the *FULL* partition key to encode the destination into it
        final Object partitionKey = PartitionRouteUtil
                .composePartitionKey(payload, channel, payloadKey);


        Map<String, Object> eventHeaders = new HashMap<String, Object>() {
            {
                put(EventBuiltinAttr.EVENT_ID.getKey(), eventSeq);
                put(EventBuiltinAttr.EVENT_KEY.getKey(), partitionKey);
                put(EventBuiltinAttr.EVENT_CLASS.getKey(), payload.getClass().getName());
                put(EventBuiltinAttr.EVENT_TRIGGER_TIME.getKey(), System.currentTimeMillis());
                put(EventBuiltinAttr.EVENT_TRIGGER_APP.getKey(), appName);
            }
        };
        eventHeaders.putAll(eventAttrs);

        String simpleBindingName = EventBindingUtils.resolveOutputBindingName(payloadClass);
        if (StringUtils.hasText(channel)) {
            EventProducer eventProducer = EventBindingUtils.getEventProducerConf(payloadClass);
            Assert.state(!(null == eventProducer || eventProducer.channels().length == 0
              || !Sets.newHashSet(eventProducer.channels()).contains(channel)), "channel not registered to trigger event");
        }
        MessageChannel messageChannel = resolver.resolveDestination(
            EventBindingUtils.composeEventChannelBeanName(simpleBindingName, channel));
        messageChannel.send(MessageBuilder.createMessage(payload, new MessageHeaders(eventHeaders)));
        return eventSeq;
    }

    /**
     * Retrieve the EventAgent object by event payload class
     * @param payloadClass the event payload class
     * @param channel the event sub channel
     * @return the EventAgent object of payload class
     * @param <T> the template type
     */
    public static <T> EventAgent<T> of(Class<T> payloadClass, String channel) {
        String agentKey = EventBindingUtils.resolveDestination(payloadClass, channel);
        EventAgent<T> eventAgent = (EventAgent<T>)agentMap.computeIfAbsent(agentKey,
          pclass -> EventAgentFactory.createAgent(payloadClass, channel));
        return eventAgent;
    }

    public static <T> EventAgent<T> of(Class<T> payloadClass) {
        return of(payloadClass, null);
    }
}
