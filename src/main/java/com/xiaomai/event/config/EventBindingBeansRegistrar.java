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

import com.xiaomai.event.annotation.EnableEventBinding;
import com.xiaomai.event.annotation.EventHandler;
import com.xiaomai.event.enums.EventBindingType;
import com.xiaomai.event.utils.EventBindingUtils;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

import java.util.Arrays;
import org.springframework.util.ReflectionUtils;


/**
 * The event binding bean registrar
 * @author baihe
 */
@Slf4j
public class EventBindingBeansRegistrar implements ImportBeanDefinitionRegistrar, BeanFactoryAware, EnvironmentAware {

    /**
     * The bean factory to retrieve bean
     */
    private DefaultListableBeanFactory beanFactory;

    /**
     * The environment
     */
    private ConfigurableEnvironment environment;

    /**
     * register the bean definition
     * @param metadata the annotation metadata
     * @param registry the bean registry
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata,
                                        BeanDefinitionRegistry registry) {
        // Get the event binding meta information from the EnableEventBinding annotation
        AnnotationAttributes attrs = AnnotatedElementUtils.getMergedAnnotationAttributes(
            ClassUtils.resolveClassName(metadata.getClassName(), null),
            EnableEventBinding.class);

        EnableEventBinding enableEventBinding = AnnotationUtils.synthesizeAnnotation(attrs,
          EnableEventBinding.class, ClassUtils.resolveClassName(metadata.getClassName(), null));

        // Register the events to publish and consume
        EventBindingUtils.registerEventBindingBeanDefinitions(
            enableEventBinding.produce(), enableEventBinding.listenerClass(),
            registry, ClassUtils.resolveClassName(metadata.getClassName(), null));

        //*IMPORTANT* replace the original BindingServiceProperties with {@link EventBindingServiceProperties}
        registry.removeBeanDefinition("spring.cloud.stream-" + BindingServiceProperties.class.getName());
        registry.removeBeanDefinition("messageHandlerMethodFactory");
    }

    /**
     * Init the bean factory
     * @param beanFactory the given bean factory
     * @throws BeansException the possible bean exception
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = (DefaultListableBeanFactory)beanFactory;
    }

    /**
     * Init the environment
     * @param environment the given environment
     */
    @Override
    public void setEnvironment(Environment environment) {
        this.environment = (ConfigurableEnvironment)environment;
    }

}
