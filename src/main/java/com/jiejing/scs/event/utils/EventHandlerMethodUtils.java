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

package com.jiejing.scs.event.utils;

import com.jiejing.scs.event.annotation.EventHandler;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;

/**
 * This class contains utility methods for handling {@link EventHandler} annotated bean
 * methods.
 *
 * @author Ilayaperumal Gopinathan
 */
public class EventHandlerMethodUtils {

	public static int inputAnnotationCount(Method method) {
		int inputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				inputAnnotationCount++;
			}
		}
		return inputAnnotationCount;
	}

	public static int outputAnnotationCount(Method method) {
		int outputAnnotationCount = 0;
		for (int parameterIndex = 0; parameterIndex < method.getParameterTypes().length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
			if (methodParameter.hasParameterAnnotation(Output.class)) {
				outputAnnotationCount++;
			}
		}
		return outputAnnotationCount;
	}

	public static void validateStreamListenerMethod(Method method, int inputAnnotationCount,
			int outputAnnotationCount, String methodAnnotatedInboundName, String methodAnnotatedOutboundName,
			boolean isDeclarative, String condition) {
		int methodArgumentsLength = method.getParameterTypes().length;
		if (!isDeclarative) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
		}
		if (StringUtils.hasText(methodAnnotatedInboundName) && StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(inputAnnotationCount == 0 && outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_INPUT_OUTPUT_METHOD_PARAMETERS);
		}
		if (StringUtils.hasText(methodAnnotatedInboundName)) {
			Assert.isTrue(inputAnnotationCount == 0, StreamListenerErrorMessages.INVALID_INPUT_VALUES);
			Assert.isTrue(outputAnnotationCount == 0,
					StreamListenerErrorMessages.INVALID_INPUT_VALUE_WITH_OUTPUT_METHOD_PARAM);
		}
		else {
			Assert.isTrue(inputAnnotationCount >= 1, StreamListenerErrorMessages.NO_INPUT_DESTINATION);
		}
		if (StringUtils.hasText(methodAnnotatedOutboundName)) {
			Assert.isTrue(outputAnnotationCount == 0, StreamListenerErrorMessages.INVALID_OUTPUT_VALUES);
		}
		if (!Void.TYPE.equals(method.getReturnType())) {
			Assert.isTrue(!StringUtils.hasText(condition),
					StreamListenerErrorMessages.CONDITION_ON_METHOD_RETURNING_VALUE);
		}
		if (isDeclarative) {
			Assert.isTrue(!StringUtils.hasText(condition),
					StreamListenerErrorMessages.CONDITION_ON_DECLARATIVE_METHOD);
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
				if (methodParameter.hasParameterAnnotation(Input.class)) {
					String inboundName = (String) AnnotationUtils
							.getValue(methodParameter.getParameterAnnotation(Input.class));
					Assert.isTrue(StringUtils.hasText(inboundName), StreamListenerErrorMessages.INVALID_INBOUND_NAME);
				}
				if (methodParameter.hasParameterAnnotation(Output.class)) {
					String outboundName = (String) AnnotationUtils
							.getValue(methodParameter.getParameterAnnotation(Output.class));
					Assert.isTrue(StringUtils.hasText(outboundName), StreamListenerErrorMessages.INVALID_OUTBOUND_NAME);
				}
			}
			if (methodArgumentsLength > 1) {
				Assert.isTrue(inputAnnotationCount + outputAnnotationCount == methodArgumentsLength,
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}

		if (!method.getReturnType().equals(Void.TYPE)) {
			if (!StringUtils.hasText(methodAnnotatedOutboundName)) {
				if (outputAnnotationCount == 0) {
					throw new IllegalArgumentException(StreamListenerErrorMessages.RETURN_TYPE_NO_OUTBOUND_SPECIFIED);
				}
				Assert.isTrue((outputAnnotationCount == 1), StreamListenerErrorMessages.RETURN_TYPE_MULTIPLE_OUTBOUND_SPECIFIED);
			}
		}
	}

	public static void validateStreamListenerMessageHandler(Method method) {
		int methodArgumentsLength = method.getParameterTypes().length;
		if (methodArgumentsLength > 1) {
			int numAnnotatedMethodParameters = 0;
			int numPayloadAnnotations = 0;
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method, parameterIndex);
				if (methodParameter.hasParameterAnnotations()) {
					numAnnotatedMethodParameters++;
				}
				if (methodParameter.hasParameterAnnotation(Payload.class)) {
					numPayloadAnnotations++;
				}
			}
			if (numPayloadAnnotations > 0) {
				Assert.isTrue(methodArgumentsLength == numAnnotatedMethodParameters && numPayloadAnnotations <= 1,
						StreamListenerErrorMessages.AMBIGUOUS_MESSAGE_HANDLER_METHOD_ARGUMENTS);
			}
		}
	}

	public static String getOutboundBindingTargetName(Method method) {
		SendTo sendTo = AnnotationUtils.findAnnotation(method, SendTo.class);
		if (sendTo != null) {
			Assert.isTrue(!ObjectUtils.isEmpty(sendTo.value()), StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			Assert.isTrue(sendTo.value().length == 1, StreamListenerErrorMessages.SEND_TO_MULTIPLE_DESTINATIONS);
			Assert.hasText(sendTo.value()[0], StreamListenerErrorMessages.SEND_TO_EMPTY_DESTINATION);
			return sendTo.value()[0];
		}
		Output output = AnnotationUtils.findAnnotation(method, Output.class);
		if (output != null) {
			Assert.isTrue(StringUtils.hasText(output.value()), StreamListenerErrorMessages.ATLEAST_ONE_OUTPUT);
			return output.value();
		}
		return null;
	}
}
