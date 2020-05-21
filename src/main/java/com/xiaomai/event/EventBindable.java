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

import com.xiaomai.event.config.EventBindingBeansRegistrar;
import com.xiaomai.event.utils.EventBindingUtils;
import net.logstash.logback.encoder.org.apache.commons.lang.StringUtils;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.aggregate.SharedBindingTargetRegistry;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binding.Bindable;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.binding.SubscribableChannelBindingTargetFactory;
import org.springframework.cloud.stream.internal.InternalPropertyNames;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * The bindable proxy of the event.
 *
 * Created by baihe on 2017/8/22.
 */
@Slf4j
public class EventBindable implements InitializingBean, Bindable {

    private final Class<?> eventPayloadClass;

    private final String channel;

    private BoundTargetHolder inputHolder = null;

    private BoundTargetHolder outputHolder = null;

    /**
     * The namespace to manage target names
     */
    @Value("${" + InternalPropertyNames.NAMESPACE_PROPERTY_NAME + ":}")
    private String namespace;

    /**
     * CONSTRUCTOR
     * @param eventPayloadClass the event payload class
     */
    public EventBindable(Class<?> eventPayloadClass, String channel) {
        this.eventPayloadClass = eventPayloadClass;
        this.channel = channel;
    }

    /**
     * Retrieve the output message channel bean.
     * Used as the *FACTORY* method of output message channel bean,
     * see {@link EventBindingBeansRegistrar#registerBeanDefinitions}
     * and {@link EventBindingUtils#registerInputBindingTargetBeanDefinition}.
     * 
     * @return the output message channel
     */
    public final MessageChannel output() {
        return (MessageChannel) outputHolder.getBoundTarget();
    }

    /**
     * Retrieve the input message channel bean.
     * Used as the *FACTORY* method of input message channel bean,
     * see {@link EventBindingBeansRegistrar#registerBeanDefinitions}
     * and {@link EventBindingUtils#registerInputBindingTargetBeanDefinition}.
     *
     * @return the input message channel
     */
    public final SubscribableChannel input() {
        return (SubscribableChannel) inputHolder.getBoundTarget();
    }

    /**
     * the event channel binding target factory
     */
    @Autowired
    @Setter
    private SubscribableChannelBindingTargetFactory eventChannelBindingTargetFactory;

    /**
     * The shared registry to hold the binding target (channel)
     */
    @Autowired(required = false)
    private SharedBindingTargetRegistry sharedBindingTargetRegistry;

    /**
     * Get the binding target (channel) from the shared registry
     * @param name the name of the binding target
     * @param bindingTargetType the binding target type
     * @return the binding target
     */
    private <T> T locateSharedBindingTarget(String name, Class<T> bindingTargetType) {
        return this.sharedBindingTargetRegistry != null
                ? this.sharedBindingTargetRegistry.get(getNamespacePrefixedBindingTargetName(name), bindingTargetType)
                : null;
    }

    /**
     * Get the absolute binding target name
     * @param name the simple target name
     * @return the absolute target name
     */
    private String getNamespacePrefixedBindingTargetName(String name) {
        return this.namespace + "." + name;
    }

    /**
     * Holds information about the adapter targets exposed by the interface proxy, as well
     * as their status.
     */
    @Data
    @AllArgsConstructor
    private final class BoundTargetHolder {

        private String name;

        /**
         * The bindable target object
         */
        private Object boundTarget;

        /**
         * The bindable flag
         */
        private boolean bindable;
    }

    /**
     * The **POST** constructor process to init the binding targets
     * @throws Exception the possible exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        String inputChannelName = EventBindingUtils.resolveInputBindingName(eventPayloadClass);
        if (StringUtils.isNotBlank(channel)) {
            inputChannelName = EventBindingUtils
                .composeEventChannelBeanName(inputChannelName, channel);
        }
        Object sharedBindingInputTarget = locateSharedBindingTarget(inputChannelName,
            SubscribableChannel.class);
        if (sharedBindingInputTarget != null) {
            inputHolder = new BoundTargetHolder(inputChannelName, sharedBindingInputTarget,
                false);
        } else {
            inputHolder = new BoundTargetHolder(inputChannelName,
                eventChannelBindingTargetFactory.createInput(inputChannelName), true);
        }

        // For OUTPUT we need configure both output & input channel
        String outputChannelName = EventBindingUtils
          .resolveOutputBindingName(eventPayloadClass);
        if (StringUtils.isNotBlank(channel)) {
            outputChannelName = EventBindingUtils
              .composeEventChannelBeanName(outputChannelName, channel);
        }
        Object sharedBindingOutputTarget = locateSharedBindingTarget(outputChannelName,
          MessageChannel.class);
        if (sharedBindingOutputTarget != null) {
            outputHolder = new BoundTargetHolder(outputChannelName, sharedBindingOutputTarget,
              false);
        } else {
            outputHolder = new BoundTargetHolder(outputChannelName,
              eventChannelBindingTargetFactory.createOutput(outputChannelName), true);
        }
    }

    /**
     * @deprecated in favor of {@link #createAndBindInputs(BindingService)}
     */
    @Override
    @Deprecated
    public void bindInputs(BindingService bindingService) {
        this.createAndBindInputs(bindingService);
    }

    /**
     * Bind the input channels
     * @param bindingService the binding service
     * @return the bindings
     */
    @Override
    public Collection<Binding<Object>> createAndBindInputs(BindingService bindingService) {
        List<Binding<Object>> bindings = new ArrayList<>();
        if (log.isDebugEnabled()) {
            log.debug(
                String.format("Binding inputs for %s:%s", this.namespace, this.getClass()));
        }

        if (inputHolder.isBindable()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Binding %s:%s:%s", this.namespace, this.getClass(),
                    inputHolder.getName()));
            }
            bindings.addAll(bindingService
                .bindConsumer(inputHolder.getBoundTarget(), inputHolder.getName()));
        }
        return bindings;
    }

    /**
     * Bind the output channels
     * @param bindingService the binding service
     */
    @Override
    public void bindOutputs(BindingService bindingService) {
        if (log.isDebugEnabled()) {
            log.debug(
                String.format("Binding outputs for %s:%s", this.namespace, this.getClass()));
        }
        if (outputHolder.isBindable()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Binding %s:%s:%s", this.namespace, this.getClass(),
                    outputHolder.getName()));
            }
            bindingService.bindProducer(outputHolder.getBoundTarget(), outputHolder.getName());
        }
    }

    /**
     * unbind the input channel
     * @param bindingService the binding service
     */
    @Override
    public void unbindInputs(BindingService bindingService) {
        if (log.isDebugEnabled()) {
            log.debug(
                String.format("Unbinding inputs for %s:%s", this.namespace, this.getClass()));
        }

        if (inputHolder.isBindable()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Unbinding %s:%s:%s", this.namespace, this.getClass(),
                    inputHolder.getName()));
            }
            bindingService.unbindConsumers(inputHolder.getName());
        }
    }

    /**
     * unbind the output channels
     * @param bindingService the binding service
     */
    @Override
    public void unbindOutputs(BindingService bindingService) {
        if (log.isDebugEnabled()) {
            log.debug(
                String.format("Unbinding outputs for %s:%s", this.namespace, this.getClass()));
        }
        if (outputHolder.isBindable()) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Unbinding %s:%s:%s", this.namespace, this.getClass(),
                    outputHolder.getName()));
            }
            bindingService.unbindProducers(outputHolder.getName());
        }
    }

    /**
     * Get the input channel names
     * @return the channel names
     */
    @Override
    public Set<String> getInputs() {
        return Sets.newHashSet(this.inputHolder.name);
    }

    /**
     * Get the output channel names
     * @return the channel names
     */
    @Override
    public Set<String> getOutputs() {
        return Sets.newHashSet(this.outputHolder.name);
    }

}
