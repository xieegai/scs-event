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

package org.baihe.scs.event.utils;

import org.baihe.scs.event.EventBindable;
import org.baihe.scs.event.annotation.EventMeta;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;

import java.lang.annotation.Annotation;
import org.springframework.util.Assert;

/**
 * Utility class for registering bean definitions for adapter targets.
 *
 * @author Marius Bogoevici
 * @author Dave Syer
 * @author Artem Bilan
 */
@Slf4j
@SuppressWarnings("deprecation")
public abstract class EventBindingBeanDefinitionRegistryUtils {

    private static final String BINDING_DELIM = "_";

    private static final String BEAN_NAME_DELIM = ".";

    private static String OUTPUT_MAGIC = "publishChannel@";

    public static void registerInputBindingTargetBeanDefinition(Class<?> eventPayloadClass,
        BeanDefinitionRegistry registry, Class<?> parentClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        if (null == eventMeta) {
            log.warn("EventMeta annotation not marked on class {}, ignored", eventPayloadClass.getName());
            return;
        }

        registerBindingTargetBeanDefinition(Input.class, eventPayloadClass, registry, parentClass);
    }

    public static void registerOutputBindingTargetBeanDefinition(Class<?> eventPayloadClass,
        BeanDefinitionRegistry registry, Class<?> parentClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        if (null == eventMeta) {
            log.warn("EventMeta annotation not marked on class {}, ignored", eventPayloadClass.getName());
            return;
        }

        registerBindingTargetBeanDefinition(Output.class, eventPayloadClass, registry, parentClass);
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

        // resolve the event bean name
        String eventBeanName = EventBindingBeanDefinitionRegistryUtils.resolveEventBeanName(eventPayloadClass);
        String bindingName = Input.class.equals(qualifier) ?
            resolveInputBindingName(eventPayloadClass) : resolveOutputBindingName(eventPayloadClass);

        if (registry.containsBeanDefinition(bindingName)) {
            log.warn("bean definition with name [{}] already exists, ignored", bindingName);
            return;
        }

        // register the factory bean to produce the binding target bean
        RootBeanDefinition rootBeanDefinition = new RootBeanDefinition();
        rootBeanDefinition.setFactoryBeanName(eventBeanName + "Bindable");
        rootBeanDefinition.setUniqueFactoryMethodName(qualifier == Input.class ? "input" : "output");
        rootBeanDefinition.addQualifier(new AutowireCandidateQualifier(qualifier, bindingName));
        registry.registerBeanDefinition(bindingName, rootBeanDefinition);

        // register the factory bean to produce the binding target bean
        RootBeanDefinition factoryBeanDefinition = new RootBeanDefinition(EventBindable.class);
        factoryBeanDefinition.addQualifier(new AutowireCandidateQualifier(Bindings.class, parentClass));
        ConstructorArgumentValues constructorArgumentValues = factoryBeanDefinition.getConstructorArgumentValues();
        constructorArgumentValues.addGenericArgumentValue(eventPayloadClass);
        if (!registry.containsBeanDefinition(eventBeanName + "Bindable")) {
            registry.registerBeanDefinition(eventBeanName + "Bindable", factoryBeanDefinition);
        }
    }

    /**
     * Resolve the input binding name for given event
     * @return the resolved input binding name
     */
    public static String resolveInputBindingName(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());

        if (StringUtils.isNotBlank(eventMeta.domain()))
            return eventMeta.domain() + BINDING_DELIM + eventMeta.name();
        return eventMeta.name();
    }


    /**
     * Resolve the output binding name for given event
     * @return the resolved output binding name
     */
    public static String resolveOutputBindingName(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());

        if (StringUtils.isNotBlank(eventMeta.domain()))
            return OUTPUT_MAGIC + eventMeta.domain() + BINDING_DELIM + eventMeta.name();
        return OUTPUT_MAGIC + eventMeta.name();
    }

    /**
     *
     * @param eventPayloadClass
     * @return
     */
    public static String resolveEventBeanName(Class<?> eventPayloadClass) {
        EventMeta eventMeta = eventPayloadClass.getAnnotation(EventMeta.class);
        Assert.state(null != eventMeta, "EventMeta annotation not marked on class " + eventPayloadClass.getName());

        if (StringUtils.isNotBlank(eventMeta.domain()))
            return eventMeta.domain() + BEAN_NAME_DELIM + eventMeta.name();
        return eventMeta.name();
    }

    /**
     * Parse the event topic from the output channel name
     * @param channelName
     * @return
     */
    public static String resolveEventName(String channelName) {
        if (channelName.startsWith(OUTPUT_MAGIC))
            return channelName.substring(OUTPUT_MAGIC.length());
        return channelName;
    }
}
