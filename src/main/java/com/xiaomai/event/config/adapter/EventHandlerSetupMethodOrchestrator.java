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
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;

/**
 * Orchestrator used for invoking the {@link EventHandler} setup method.
 *
 * By default {@link EventHandlerAnnotationBeanPostProcessor} will use an internal implementation
 * of this interface to invoke {@link StreamListenerParameterAdapter}s and {@link StreamListenerResultAdapter}s
 * or handler mappings on the method annotated with {@link EventHandler}.
 *
 * By providing a different implementation of this interface and registering it as a Spring Bean in the
 * context, one can override the default invocation strategies used by the {@link EventHandlerAnnotationBeanPostProcessor}.
 * A typical usecase for such overriding can happen when a downstream {@link org.springframework.cloud.stream.binder.Binder}
 * implementation wants to change the way in which any of the default EventHandler handling needs to be changed in a
 * custom manner.
 *
 * When beans of this interface are present in the context, they get priority in the {@link EventHandlerAnnotationBeanPostProcessor}
 * before falling back to the default implementation.
 *
 * @see EventHandler
 * @see EventHandlerAnnotationBeanPostProcessor
 *
 * @author Soby Chacko
 */
public interface EventHandlerSetupMethodOrchestrator {

	/**
	 * Checks the method annotated with {@link EventHandler} to see if this implementation
	 * can successfully orchestrate this method.
	 *
	 * @param method annotated with {@link EventHandler}
	 * @return true if this implementation can orchestrate this method, false otherwise
	 */
	boolean supports(Method method);

	/**
	 * Method that allows custom orchestration on the {@link EventHandler} setup method.
	 *
	 * @param eventHandler reference to the {@link EventHandler} annotation on the method
	 * @param method annotated with {@link EventHandler}
	 * @param bean that contains the EventHandler method
	 *
	 */
	void orchestrateStreamListenerSetupMethod(EventHandler eventHandler, Method method, Object bean);

	/**
	 * Default implementation for adapting each of the incoming method arguments using an available
	 * {@link StreamListenerParameterAdapter} and provide the adapted collection of arguments back to the caller.
	 *
	 * @param method annotated with {@link EventHandler}
	 * @param inboundName inbound adapter
	 * @param applicationContext spring application context
	 * @param streamListenerParameterAdapters used for adapting the method arguments
	 * @return adapted incoming arguments
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	default Object[] adaptAndRetrieveInboundArguments(Method method, String inboundName,
                                                      ApplicationContext applicationContext,
                                                      StreamListenerParameterAdapter... streamListenerParameterAdapters) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Input.class));
			}
			else if (methodParameter.hasParameterAnnotation(Output.class)) {
				targetReferenceValue = AnnotationUtils.getValue(methodParameter.getParameterAnnotation(Output.class));
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue, "Annotation value must be a String");
				Object targetBean = applicationContext.getBean((String) targetReferenceValue);
				// Iterate existing parameter adapters first
				for (StreamListenerParameterAdapter streamListenerParameterAdapter : streamListenerParameterAdapters) {
					if (streamListenerParameterAdapter.supports(targetBean.getClass(), methodParameter)) {
						arguments[parameterIndex] = streamListenerParameterAdapter.adapt(targetBean, methodParameter);
						break;
					}
				}
				if (arguments[parameterIndex] == null && parameterType.isAssignableFrom(targetBean.getClass())) {
					arguments[parameterIndex] = targetBean;
				}
				Assert.notNull(arguments[parameterIndex], "Cannot convert argument " + parameterIndex + " of " + method
						+ "from " + targetBean.getClass() + " to " + parameterType);
			}
			else {
				throw new IllegalStateException(StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

}
