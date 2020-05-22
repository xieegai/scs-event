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

package com.xiaomai.event.utils;

import com.xiaomai.event.annotation.EventHandler;
import com.xiaomai.event.annotation.EventMeta;
import com.xiaomai.event.EventBindable;
import com.xiaomai.event.annotation.EventConf;
import com.xiaomai.event.enums.EventBindingType;
import java.util.Arrays;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Utility class for registering bean definitions for adapter targets.
 *
 * @author Marius Bogoevici
 * @author Dave Syer
 * @author Artem Bilan
 */
@Slf4j
@SuppressWarnings("deprecation")
public abstract class EventBindingUtils {

    private static final String DOMAIN_DELIM = ".";

    private static final String CHANNEL_DELIM = "___";

    private static String OUTPUT_MAGIC = "output@";

    private static String BINDABLE_MAGIC = "-bindable";

    private static Map<String, Class<?>> channelEventMap = new ConcurrentHashMap<>();

    private static Map<Class<?>, Map<EventBindingType, EventConf>> eventConfMap = new ConcurrentHashMap<>();

    private static Map<Class<?>, EventHandler> eventConsumerConfMap = new ConcurrentHashMap<>();

    private static Map<String, EventBindingType> bindingTypeMap = new ConcurrentHashMap<>();

    /**
     * Cache the event (grobal) config
     * @param eventConf the event config to cache
     */
    public static void cacheEventConfig(EventConf eventConf, EventBindingType eventBindingType) {
        eventConfMap.putIfAbsent(eventConf.event(), new ConcurrentHashMap<>());
        eventConfMap.get(eventConf.event()).putIfAbsent(eventBindingType, eventConf);
    }

    /**
     * Cache the event consumer config
     * @param eventHandler the event consumer config to cache
     */
    public static void cacheEventHandler(EventHandler eventHandler) {
        eventConsumerConfMap.putIfAbsent(eventHandler.value(), eventHandler);

    }

    public static void registerEventBindingBeanDefinitions(
        EventConf[] produceEvents, EventConf[] consumeEvents,
        BeanDefinitionRegistry registry, Class<?> parentClass) {
        Arrays.stream(produceEvents).forEach(e -> EventBindingUtils.cacheEventConfig(e,
            EventBindingType.OUTPUT));
        Arrays.stream(consumeEvents).forEach(e -> EventBindingUtils.cacheEventConfig(e,
            EventBindingType.INPUT));

        eventConfMap.forEach((eventPayloadClass, confMap) -> {
            if (confMap.containsKey(EventBindingType.INPUT)) {
                EventConf inputEventConf = confMap.get(EventBindingType.INPUT);
                registerInputBindingTargetBeanDefinition(eventPayloadClass, inputEventConf, registry, parentClass);
            }
            if (confMap.containsKey(EventBindingType.OUTPUT)) {
                EventConf outputEventConf = confMap.get(EventBindingType.OUTPUT);
                registerOutputBindingTargetBeanDefinition(eventPayloadClass, outputEventConf, registry, parentClass);
            }
        });
    }

    private static void registerInputBindingTargetBeanDefinition(Class<?> eventPayloadClass,
        EventConf eventConf, BeanDefinitionRegistry registry, Class<?> parentClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        if (null == eventMeta) {
            log.warn("EventMeta annotation not marked on class {}, ignored", eventPayloadClass.getName());
            return;
        }
        if (null != eventConf && eventConf.channels().length > 0) {
            registerBindingTargetBeanDefinition(Input.class, eventPayloadClass,
                eventConf.channels(), registry, parentClass);
        } else {
            registerBindingTargetBeanDefinition(Input.class, eventPayloadClass, registry, parentClass);
        }
    }

    private static void registerOutputBindingTargetBeanDefinition(Class<?> eventPayloadClass,
        EventConf eventConf, BeanDefinitionRegistry registry, Class<?> parentClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        if (null == eventMeta) {
            log.warn("EventMeta annotation not marked on class {}, ignored", eventPayloadClass.getName());
            return;
        }
        if (null != eventConf && eventConf.channels().length > 0) {
            registerBindingTargetBeanDefinition(Output.class, eventPayloadClass,
                eventConf.channels(), registry, parentClass);
        } else {
            registerBindingTargetBeanDefinition(Output.class, eventPayloadClass, registry, parentClass);
        }
    }

    /**
     * Register a bean definition for a output/input binding bean by construct its *Bindable* factory bean
     * reference.
     *
     * @param qualifier the {@link Output} and {@link Input} annotation qualifier
     * @param registry the bean definition registry
     */
    private static void registerBindingTargetBeanDefinition(Class<? extends Annotation> qualifier,
        Class<?> eventPayloadClass,
        BeanDefinitionRegistry registry, Class<?> parentClass) {
        registerBindingTargetBeanDefinition(qualifier, eventPayloadClass, new String[0], registry, parentClass);
    }
    private static void registerBindingTargetBeanDefinition(Class<? extends Annotation> qualifier,
        Class<?> eventPayloadClass,
        String[] channels,
        BeanDefinitionRegistry registry,
        Class<?> parentClass) {

        // resolve the event bean name
        String eventBeanName = EventBindingUtils.resolveEventBeanName(eventPayloadClass);
        String bindingName = Input.class.equals(qualifier) ?
            resolveInputBindingName(eventPayloadClass) : resolveOutputBindingName(eventPayloadClass);
        EventBindingType eventBindingType = EventBindingType.fromAnnotation(qualifier);

        if (registry.containsBeanDefinition(bindingName)) {
            log.warn("bean definition with name [{}] already exists, ignored", bindingName);
            return;
        }

        // cache the register event payload class on binding name
        channelEventMap.putIfAbsent(resolveEventName(bindingName), eventPayloadClass);

        // register the factory bean to produce the binding target bean
        final String bindableBeanName = eventBeanName + BINDABLE_MAGIC;
        if (channels != null && channels.length > 0) {
            for (String channel: channels) {
                if (!StringUtils.hasText(channel)) {
                    continue;
                }
                final String channelBeanName = bindableBeanName + CHANNEL_DELIM + channel;
                final String channelBindingName = EventBindingUtils.composeEventChannelBeanName(bindingName, channel);

                // register the factory bean to produce the binding target bean
                RootBeanDefinition rootBeanDefinition = buildBindingBeanDefinition(channelBeanName, qualifier, channelBindingName);
                registry.registerBeanDefinition(channelBindingName, rootBeanDefinition);

                // register the factory bean to produce the binding target bean
                if (!registry.containsBeanDefinition(channelBeanName)) {
                    RootBeanDefinition factoryBeanDefinition
                        = buildBindingFactoryBeanDefinition(eventPayloadClass, channel, eventBindingType, parentClass);
                    registry.registerBeanDefinition(channelBeanName, factoryBeanDefinition);
                    bindingTypeMap.put(channelBeanName, eventBindingType);
                } else if((!bindingTypeMap.get(channelBeanName).equals(EventBindingType.BOTH))
                    && (!bindingTypeMap.get(channelBeanName).equals(eventBindingType))) {
                    RootBeanDefinition factoryBeanDefinition
                        = buildBindingFactoryBeanDefinition(eventPayloadClass, channel, EventBindingType.BOTH, parentClass);
                    registry.registerBeanDefinition(channelBeanName, factoryBeanDefinition);
                    bindingTypeMap.put(channelBeanName, EventBindingType.BOTH);
                }
            }
        } else {
            // register the factory bean to produce the binding target bean
            RootBeanDefinition rootBeanDefinition = buildBindingBeanDefinition(bindableBeanName, qualifier, bindingName);
            registry.registerBeanDefinition(bindingName, rootBeanDefinition);

            // register the factory bean to produce the binding target bean
            if (!registry.containsBeanDefinition(bindableBeanName)) {
                RootBeanDefinition factoryBeanDefinition
                    = buildBindingFactoryBeanDefinition(eventPayloadClass, null, eventBindingType, parentClass);
                registry.registerBeanDefinition(bindableBeanName, factoryBeanDefinition);
                bindingTypeMap.put(bindableBeanName, eventBindingType);
            } else if((!bindingTypeMap.get(bindableBeanName).equals(EventBindingType.BOTH))
                && (!bindingTypeMap.get(bindableBeanName).equals(eventBindingType))) {
                RootBeanDefinition factoryBeanDefinition
                    = buildBindingFactoryBeanDefinition(eventPayloadClass, null, EventBindingType.BOTH, parentClass);
                registry.registerBeanDefinition(bindableBeanName, factoryBeanDefinition);
                bindingTypeMap.put(bindableBeanName, EventBindingType.BOTH);
            }
        }
    }

    private static RootBeanDefinition buildBindingBeanDefinition(String beanName, Class<? extends Annotation> qualifier, String bindingName) {
        RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
        rootBeanDefinition.setFactoryBeanName(beanName);
        rootBeanDefinition.setUniqueFactoryMethodName(Input.class.equals(qualifier) ? "input" : "output");
        rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(qualifier, bindingName));
        return rootBeanDefinition;
    }

    private static RootBeanDefinition buildBindingFactoryBeanDefinition(Class<?> eventPayloadClass, String channel,
        EventBindingType eventBindingType, Class<?> parentClass) {
        // register the factory bean to produce the binding target bean
        RootBeanDefinition factoryBeanDefinition = new RootBeanDefinition(EventBindable.class);
        factoryBeanDefinition.addQualifier(new AutowireCandidateQualifier(Bindings.class, parentClass));
        ConstructorArgumentValues constructorArgumentValues = factoryBeanDefinition.getConstructorArgumentValues();
        constructorArgumentValues.addGenericArgumentValue(eventPayloadClass);
        constructorArgumentValues.addGenericArgumentValue(eventBindingType);
        constructorArgumentValues.addGenericArgumentValue(channel);
        return factoryBeanDefinition;
    }

    /**
     * Resolve the input binding name for given event
     * @param eventPayloadClass the event payload class
     * @return the resolved input binding name
     */
    public static String resolveInputBindingName(Class<?> eventPayloadClass) {
        return resolveEventBeanName(eventPayloadClass);
    }

    private static String resolveEventBeanName(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());

        if (StringUtils.hasText(eventMeta.domain()))
            return eventMeta.domain() + DOMAIN_DELIM + eventMeta.name();
        return eventMeta.name();
    }

    /**
     * Resolve the output binding name for given event
     * @param eventPayloadClass the event payload class
     * @return the resolved output binding name
     */
    public static String resolveOutputBindingName(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());

        String bindingName = eventMeta.name();
        if (StringUtils.hasText(eventMeta.domain())) {
            bindingName = eventMeta.domain() + DOMAIN_DELIM + bindingName;
        }

        return OUTPUT_MAGIC + bindingName;
    }

    /**
     * resolve the event meta
     * @param eventPayloadClass the event payload class
     * @return the cached event meta
     */
    public static EventMeta resolveEventMeta(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());
        return eventMeta;
    }

    /**
     * Parse the event topic from the output channel name
     * @param bindingName the FULL binding name
     * @return the event name
     */
    public static String resolveEventName(String bindingName) {
        String eventName = String.valueOf(bindingName);
        if (eventName.startsWith(OUTPUT_MAGIC))
            eventName = eventName.substring(OUTPUT_MAGIC.length());
        if (eventName.contains(CHANNEL_DELIM)) {
            eventName = eventName.substring(0, eventName.indexOf(CHANNEL_DELIM));
        }
        return eventName;
    }

    public static String resolveEventChannel(String bindingName) {
        String channel = String.valueOf(bindingName);
        if (channel.contains(CHANNEL_DELIM)) {
            return channel.substring(channel.indexOf(CHANNEL_DELIM) + CHANNEL_DELIM.length());
        }
        return null;
    }

    /**
     * Get the event payload class by binding name
     * @param bindingName the binding name
     * @return the cached event payload Class
     */
    public static Class<?> getEventPayloadClass(String bindingName) {
        return channelEventMap.get(bindingName);
    }

    public static EventConf getEventConf(Class<?> eventPayloadClass, EventBindingType eventBindingType) {
        return eventConfMap.getOrDefault(eventPayloadClass, Collections.emptyMap()).get(eventBindingType);
    }

    public static EventConf getEventConf(String bindingName) {
        Class<?> eventPayloadClass = getEventPayloadClass(bindingName);
        if (null == eventPayloadClass) {
            return null;
        }
        EventBindingType eventBindingType = bindingName.startsWith(OUTPUT_MAGIC)?
            EventBindingType.OUTPUT: EventBindingType.INPUT;
        return getEventConf(eventPayloadClass, eventBindingType);
    }

    public static EventHandler getEventConsumerConf(Class<?> eventPayloadClass) {
        return eventConsumerConfMap.get(eventPayloadClass);
    }

    public static String composeEventChannelBeanName(String simpleBindingName, String channel) {
        if (StringUtils.hasText(channel)) {
            return simpleBindingName + CHANNEL_DELIM + channel;
        }
        return simpleBindingName;
    }
}
