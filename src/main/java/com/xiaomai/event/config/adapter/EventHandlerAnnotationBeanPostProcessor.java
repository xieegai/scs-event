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

package com.xiaomai.event.config.adapter;

import com.xiaomai.event.annotation.EventHandler;
import com.xiaomai.event.utils.EventBindingUtils;
import com.xiaomai.event.utils.EventHandlerMethodUtils;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.config.SpringIntegrationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.*;

import java.lang.reflect.Method;
import java.util.*;

/**
 * {@link BeanPostProcessor} that handles {@link EventHandler} annotations found on bean
 * methods.
 *
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 */
public class EventHandlerAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware,
        SmartInitializingSingleton, BeanFactoryAware {

    private static final SpelExpressionParser SPEL_EXPRESSION_PARSER = new SpelExpressionParser();

    private final MultiValueMap<String, StreamListenerHandlerMethodMapping> mappedListenerMethods = new LinkedMultiValueMap<>();

    // == dependencies that are injected in 'afterSingletonsInstantiated' to avoid early initialization
    private DestinationResolver<MessageChannel> binderAwareChannelResolver;

    private MessageHandlerMethodFactory messageHandlerMethodFactory;

    private SpringIntegrationProperties springIntegrationProperties;
    // == end dependencies

    private final Set<Runnable> streamListenerCallbacks = new HashSet<>();

    private ConfigurableApplicationContext applicationContext;

    private BeanExpressionResolver resolver;

    private BeanExpressionContext expressionContext;

    private DefaultListableBeanFactory beanFactory;

    private Set<EventHandlerSetupMethodOrchestrator> eventHandlerSetupMethodOrchestrators = new LinkedHashSet<>();

    @Override
    public final void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        this.resolver = this.applicationContext.getBeanFactory().getBeanExpressionResolver();
        this.expressionContext = new BeanExpressionContext(this.applicationContext.getBeanFactory(), null);
    }

    @Override
    public final void afterSingletonsInstantiated() {
        this.injectAndPostProcessDependencies();
        EvaluationContext evaluationContext = IntegrationContextUtils.getEvaluationContext(this.applicationContext.getBeanFactory());
        for (Map.Entry<String, List<StreamListenerHandlerMethodMapping>> mappedBindingEntry : mappedListenerMethods
                .entrySet()) {
            ArrayList<DispatchingEventHandlerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper> handlers = new ArrayList<>();
            for (StreamListenerHandlerMethodMapping mapping : mappedBindingEntry.getValue()) {
                final InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
                        .createInvocableHandlerMethod(mapping.getTargetBean(),
                                checkProxy(mapping.getMethod(), mapping.getTargetBean()));
                EventHandlerMessageHandler streamListenerMessageHandler = new EventHandlerMessageHandler(
                        invocableHandlerMethod, resolveExpressionAsBoolean(mapping.getCopyHeaders(), "copyHeaders"),
                        springIntegrationProperties.getMessageHandlerNotPropagatedHeaders());
                streamListenerMessageHandler.setApplicationContext(this.applicationContext);
                streamListenerMessageHandler.setBeanFactory(this.applicationContext.getBeanFactory());
                if (StringUtils.hasText(mapping.getDefaultOutputChannel())) {
                    streamListenerMessageHandler.setOutputChannelName(mapping.getDefaultOutputChannel());
                }
                streamListenerMessageHandler.afterPropertiesSet();
                if (StringUtils.hasText(mapping.getCondition())) {
                    String conditionAsString = resolveExpressionAsString(mapping.getCondition(), "condition");
                    Expression condition = SPEL_EXPRESSION_PARSER.parseExpression(conditionAsString);
                    handlers.add(
                            new DispatchingEventHandlerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper(
                                    condition, streamListenerMessageHandler));
                } else {
                    handlers.add(
                            new DispatchingEventHandlerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper(
                                    null, streamListenerMessageHandler));
                }
            }
            if (handlers.size() > 1) {
                for (DispatchingEventHandlerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper handler : handlers) {
                    Assert.isTrue(handler.isVoid(), StreamListenerErrorMessages.MULTIPLE_VALUE_RETURNING_METHODS);
                }
            }
            AbstractReplyProducingMessageHandler handler;

            if (handlers.size() > 1 || handlers.get(0).getCondition() != null) {
                handler = new DispatchingEventHandlerMessageHandler(handlers, evaluationContext);
            } else {
                handler = handlers.get(0).getStreamListenerMessageHandler();
            }
            handler.setApplicationContext(this.applicationContext);
            handler.setChannelResolver(this.binderAwareChannelResolver);
            handler.afterPropertiesSet();
            this.applicationContext.getBeanFactory().registerSingleton(handler.getClass().getSimpleName() + handler.hashCode(), handler);

            mappedBindingEntry.getValue().forEach(v -> {
                String bindingNameWithChannel = mappedBindingEntry.getKey();
                applicationContext.getBean(bindingNameWithChannel, SubscribableChannel.class).subscribe(handler);
            });
        }
        this.mappedListenerMethods.clear();
    }

    @Override
    public final Object postProcessAfterInitialization(Object bean, final String beanName) throws BeansException {
        Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
        // only process the EventListener registered in {@link EnableEventBinding}
        if (EventBindingUtils.isListenerClassRegistered(targetClass)) {
            Method[] uniqueDeclaredMethods = ReflectionUtils.getUniqueDeclaredMethods(targetClass);
            for (Method method : uniqueDeclaredMethods) {
                EventHandler eventHandler = AnnotatedElementUtils
                    .findMergedAnnotation(method, EventHandler.class);
                if (eventHandler != null) {
                    if (!method.isBridge()) {
                        streamListenerCallbacks.add(() -> {
                            Assert.isTrue(method.getAnnotation(Input.class) == null,
                                StreamListenerErrorMessages.INPUT_AT_STREAM_LISTENER);
                            this.doPostProcess(eventHandler, method, bean);
                        });
                    }
                }
            }
        }
        return bean;
    }

    /**
     * Extension point, allowing subclasses to customize the {@link EventHandler}
     * annotation detected by the postprocessor.
     *
     * @param originalAnnotation the original annotation
     * @param annotatedMethod    the method on which the annotation has been found
     * @return the postprocessed {@link EventHandler} annotation
     */
    protected EventHandler postProcessAnnotation(EventHandler originalAnnotation, Method annotatedMethod) {
        return originalAnnotation;
    }

    private void doPostProcess(EventHandler eventHandler, Method method, Object bean) {
        eventHandler = postProcessAnnotation(eventHandler, method);
        Optional<EventHandlerSetupMethodOrchestrator> eventHandlerSetupMethodOrchestratorAvailable =
                eventHandlerSetupMethodOrchestrators.stream()
                        .filter(t -> t.supports(method))
                        .findFirst();
        Assert.isTrue(eventHandlerSetupMethodOrchestratorAvailable.isPresent(),
                "A matching StreamListenerSetupMethodOrchestrator must be present");
        EventHandlerSetupMethodOrchestrator eventHandlerSetupMethodOrchestrator = eventHandlerSetupMethodOrchestratorAvailable.get();
        eventHandlerSetupMethodOrchestrator.orchestrateStreamListenerSetupMethod(eventHandler, method, bean);
//        EventListenerCache.registerListenedEvent(eventHandler.value(), method, this.applicationContext);
    }

    private Method checkProxy(Method methodArg, Object bean) {
        Method method = methodArg;
        if (AopUtils.isJdkDynamicProxy(bean)) {
            try {
                // Found a @EventHandler method on the target class for this JDK proxy
                // ->
                // is it also present on the proxy itself?
                method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
                Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
                for (Class<?> iface : proxiedInterfaces) {
                    try {
                        method = iface.getMethod(method.getName(), method.getParameterTypes());
                        break;
                    } catch (NoSuchMethodException noMethod) {
                    }
                }
            } catch (SecurityException ex) {
                ReflectionUtils.handleReflectionException(ex);
            } catch (NoSuchMethodException ex) {
                throw new IllegalStateException(String.format(
                        "@EventHandler method '%s' found on bean target class '%s', "
                                + "but not found in any interface(s) for bean JDK proxy. Either "
                                + "pull the method up to an interface or switch to subclass (CGLIB) "
                                + "proxies by setting proxy-target-class/proxyTargetClass attribute to 'true'",
                        method.getName(), method.getDeclaringClass().getSimpleName()), ex);
            }
        }
        return method;
    }

    private String resolveExpressionAsString(String value, String property) {
        Object resolved = resolveExpression(value);
        if (resolved instanceof String) {
            return (String) resolved;
        } else {
            throw new IllegalStateException("Resolved " + property + " to [" + resolved.getClass()
                    + "] instead of String for [" + value + "]");
        }
    }

    private boolean resolveExpressionAsBoolean(String value, String property) {
        Object resolved = resolveExpression(value);
        if (resolved == null) {
            return false;
        } else if (resolved instanceof String) {
            return Boolean.parseBoolean((String) resolved);
        } else if (resolved instanceof Boolean) {
            return (Boolean) resolved;
        } else {
            throw new IllegalStateException("Resolved " + property + " to [" + resolved.getClass()
                    + "] instead of String or Boolean for [" + value + "]");
        }
    }

    private String resolveExpression(String value) {
        String resolvedValue = this.applicationContext.getBeanFactory().resolveEmbeddedValue(value);
        if (resolvedValue.startsWith("#{") && value.endsWith("}")) {
            resolvedValue = (String) this.resolver.evaluate(resolvedValue, this.expressionContext);
        }
        return resolvedValue;
    }

    /**
     * This operations ensures that required dependencies are not accidentally injected early given that this bean is BPP.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void injectAndPostProcessDependencies() {
        Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters = this.applicationContext.getBeansOfType(StreamListenerParameterAdapter.class).values();
        Collection<StreamListenerResultAdapter> streamListenerResultAdapters = this.applicationContext.getBeansOfType(StreamListenerResultAdapter.class).values();
        this.binderAwareChannelResolver = this.applicationContext.getBean(
            BinderAwareChannelResolver.class);
        this.messageHandlerMethodFactory = this.applicationContext.getBean("eventHandlerMethodFactory", MessageHandlerMethodFactory.class);
        this.springIntegrationProperties = this.applicationContext.getBean(SpringIntegrationProperties.class);

        this.eventHandlerSetupMethodOrchestrators.addAll(
                this.applicationContext.getBeansOfType(EventHandlerSetupMethodOrchestrator.class).values());

        //Default orchestrator for EventHandler method invocation is added last into the LinkedHashSet.
        this.eventHandlerSetupMethodOrchestrators.add(new DefaultEventHandlerSetupMethodOrchestrator(this.applicationContext,
                streamListenerParameterAdapters, streamListenerResultAdapters));

        this.streamListenerCallbacks.forEach(Runnable::run);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = (DefaultListableBeanFactory) beanFactory;
    }

    private class StreamListenerHandlerMethodMapping {

        private final Object targetBean;

        private final Method method;

        private final String condition;

        private final String defaultOutputChannel;

        private final String copyHeaders;

        private final String channel;

        StreamListenerHandlerMethodMapping(Object targetBean, Method method, String channel,
            String condition, String defaultOutputChannel, String copyHeaders) {
            this.targetBean = targetBean;
            this.method = method;
            this.condition = condition;
            this.defaultOutputChannel = defaultOutputChannel;
            this.copyHeaders = copyHeaders;
            this.channel = channel;
        }

        Object getTargetBean() {
            return targetBean;
        }

        Method getMethod() {
            return method;
        }

        String getCondition() {
            return condition;
        }

        String getDefaultOutputChannel() {
            return defaultOutputChannel;
        }

        public String getCopyHeaders() {
            return this.copyHeaders;
        }

        public String getChannel() {
            return this.channel;
        }
    }

    @SuppressWarnings("rawtypes")
    private class DefaultEventHandlerSetupMethodOrchestrator implements EventHandlerSetupMethodOrchestrator {

        private final ConfigurableApplicationContext applicationContext;

        private final Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters;

        private final Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

        private DefaultEventHandlerSetupMethodOrchestrator(ConfigurableApplicationContext applicationContext, Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters, Collection<StreamListenerResultAdapter> streamListenerResultAdapters) {
            this.applicationContext = applicationContext;
            this.streamListenerParameterAdapters = streamListenerParameterAdapters;
            this.streamListenerResultAdapters = streamListenerResultAdapters;
        }

        @Override
        public void orchestrateStreamListenerSetupMethod(EventHandler eventHandler, Method method, Object bean) {
            Class<?> eventPayloadClass = eventHandler.value();
            String methodAnnotatedInboundName = EventBindingUtils.resolveInputBindingName(eventPayloadClass);

            String methodAnnotatedOutboundName = EventHandlerMethodUtils.getOutboundBindingTargetName(method);
            int inputAnnotationCount = EventHandlerMethodUtils.inputAnnotationCount(method);
            int outputAnnotationCount = EventHandlerMethodUtils.outputAnnotationCount(method);
            boolean isDeclarative = checkDeclarativeMethod(method, methodAnnotatedInboundName, methodAnnotatedOutboundName);
            EventHandlerMethodUtils.validateStreamListenerMethod(method,
                    inputAnnotationCount, outputAnnotationCount,
                    methodAnnotatedInboundName, methodAnnotatedOutboundName,
                    isDeclarative, eventHandler.condition());
            if (isDeclarative) {
                StreamListenerParameterAdapter[] toSlpaArray = new StreamListenerParameterAdapter[this.streamListenerParameterAdapters.size()];
                Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(method, methodAnnotatedInboundName,
                        this.applicationContext,
                        this.streamListenerParameterAdapters.toArray(toSlpaArray));
                invokeStreamListenerResultAdapter(method, bean, methodAnnotatedOutboundName, adaptedInboundArguments);
            } else {
                registerHandlerMethodOnListenedChannel(method, eventHandler, bean);
            }
        }

        @Override
        public boolean supports(Method method) {
            //default catch all orchestrator
            return true;
        }

        @SuppressWarnings("unchecked")
        private void invokeStreamListenerResultAdapter(Method method, Object bean, String outboundName, Object... arguments) {
            try {
                if (Void.TYPE.equals(method.getReturnType())) {
                    method.invoke(bean, arguments);
                } else {
                    Object result = method.invoke(bean, arguments);
                    if (!StringUtils.hasText(outboundName)) {
                        for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
                            MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
                            if (methodParameter.hasParameterAnnotation(Output.class)) {
                                outboundName = methodParameter.getParameterAnnotation(Output.class).value();
                            }
                        }
                    }
                    Object targetBean = this.applicationContext.getBean(outboundName);
                    for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
                        if (streamListenerResultAdapter.supports(result.getClass(), targetBean.getClass())) {
                            streamListenerResultAdapter.adapt(result, targetBean);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                throw new BeanInitializationException("Cannot setup EventHandler for " + method, e);
            }
        }

        private void registerHandlerMethodOnListenedChannel(Method method, EventHandler eventHandler, Object bean) {
            Assert.notNull(eventHandler.value(), "The adapter eventClass cannot be null");
            final String defaultOutputChannel = EventHandlerMethodUtils.getOutboundBindingTargetName(method);
            if (Void.TYPE.equals(method.getReturnType())) {
                Assert.isTrue(StringUtils.isEmpty(defaultOutputChannel),
                        "An output channel cannot be specified for a method that does not return a value");
            } else {
                Assert.isTrue(!StringUtils.isEmpty(defaultOutputChannel),
                        "An output channel must be specified for a method that can return a value");
            }
            EventHandlerMethodUtils.validateStreamListenerMessageHandler(method);

            if (eventHandler.channels().length > 0) {
                for (String channel: eventHandler.channels()) {
                    if (StringUtils.hasText(channel)) {
                        String simpleBindingName = EventBindingUtils.resolveInputBindingName(eventHandler.value());
                        String bindingName = EventBindingUtils.composeEventChannelBeanName(simpleBindingName, channel);
                        mappedListenerMethods.add(bindingName,
                            new StreamListenerHandlerMethodMapping(bean, method, channel,
                                eventHandler.condition(), defaultOutputChannel,
                                eventHandler.copyHeaders()));
                    }
                }
            } else {
                mappedListenerMethods.add(EventBindingUtils.resolveInputBindingName(eventHandler.value()),
                    new StreamListenerHandlerMethodMapping(bean, method, null,
                        eventHandler.condition(), defaultOutputChannel,
                        eventHandler.copyHeaders()));
            }
        }

        private boolean checkDeclarativeMethod(Method method, String methodAnnotatedInboundName, String methodAnnotatedOutboundName) {
            int methodArgumentsLength = method.getParameterTypes().length;
            for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
                MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
                if (methodParameter.hasParameterAnnotation(Input.class)) {
                    String inboundName = (String) AnnotationUtils
                            .getValue(methodParameter.getParameterAnnotation(Input.class));
                    Assert.isTrue(StringUtils.hasText(inboundName), StreamListenerErrorMessages.INVALID_INBOUND_NAME);
                    Assert.isTrue(isDeclarativeMethodParameter(inboundName, methodParameter),
                            StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
                    return true;
                } else if (methodParameter.hasParameterAnnotation(Output.class)) {
                    String outboundName = (String) AnnotationUtils
                            .getValue(methodParameter.getParameterAnnotation(Output.class));
                    Assert.isTrue(StringUtils.hasText(outboundName), StreamListenerErrorMessages.INVALID_OUTBOUND_NAME);
                    Assert.isTrue(isDeclarativeMethodParameter(outboundName, methodParameter),
                            StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
                    return true;
                } else if (StringUtils.hasText(methodAnnotatedOutboundName)) {
                    return isDeclarativeMethodParameter(methodAnnotatedOutboundName, methodParameter);
                } else if (StringUtils.hasText(methodAnnotatedInboundName)) {
                    return isDeclarativeMethodParameter(methodAnnotatedInboundName, methodParameter);
                }
            }
            return false;
        }

        /**
         * Determines if method parameters signify an imperative or declarative listener definition.
         * <br>
         * Imperative - where handler method is invoked on each message by the handler infrastructure provided
         * by the framework
         * <br>
         * Declarative - where handler  is provided by the method itself.
         * <br>
         * Declarative method parameter could either be {@link MessageChannel} or any other Object for which
         * there is a {@link StreamListenerParameterAdapter} (i.e., {@link reactor.core.publisher.Flux}). Declarative method is invoked only
         * once during initialization phase.
         */
        @SuppressWarnings("unchecked")
        private boolean isDeclarativeMethodParameter(String targetBeanName, MethodParameter methodParameter) {
            boolean declarative = false;
            if (!methodParameter.getParameterType().isAssignableFrom(Object.class) && this.applicationContext.containsBean(targetBeanName)) {
                declarative = MessageChannel.class.isAssignableFrom(methodParameter.getParameterType());
                if (!declarative) {
                    Class<?> targetBeanClass = this.applicationContext.getType(targetBeanName);
                    declarative = this.streamListenerParameterAdapters.stream()
                            .anyMatch(slpa -> slpa.supports(targetBeanClass, methodParameter));
                }
            }
            return declarative;
        }
    }
}
