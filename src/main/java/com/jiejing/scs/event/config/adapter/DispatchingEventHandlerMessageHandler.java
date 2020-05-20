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

package com.jiejing.scs.event.config.adapter;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An {@link AbstractReplyProducingMessageHandler} that delegates to a collection of
 * internal {@link ConditionalStreamListenerMessageHandlerWrapper} instances, executing the ones that
 * match the given expression.
 *
 * @author Marius Bogoevici
 * @since 1.2
 */
final class DispatchingEventHandlerMessageHandler extends AbstractReplyProducingMessageHandler {

	private final List<ConditionalStreamListenerMessageHandlerWrapper> handlerMethods;

	private final boolean evaluateExpressions;

	private final EvaluationContext evaluationContext;

	DispatchingEventHandlerMessageHandler(Collection<ConditionalStreamListenerMessageHandlerWrapper> handlerMethods,
										  EvaluationContext evaluationContext) {
		Assert.notEmpty(handlerMethods, "'handlerMethods' cannot be empty");
		this.handlerMethods = Collections.unmodifiableList(new ArrayList<>(handlerMethods));
		boolean evaluateExpressions = false;
		for (ConditionalStreamListenerMessageHandlerWrapper handlerMethod : handlerMethods) {
			if (handlerMethod.getCondition() != null) {
				evaluateExpressions = true;
				break;
			}
		}
		this.evaluateExpressions = evaluateExpressions;
		if (evaluateExpressions) {
			Assert.notNull(evaluationContext, "'evaluationContext' cannot be null if conditions are used");
		}
		this.evaluationContext = evaluationContext;
	}

	@Override
	protected boolean shouldCopyRequestHeaders() {
		return false;
	}

	@Override
	protected Object handleRequestMessage(Message<?> requestMessage) {
		List<ConditionalStreamListenerMessageHandlerWrapper> matchingHandlers = this.evaluateExpressions ? findMatchingHandlers(requestMessage) : this.handlerMethods;
		if (matchingHandlers.size() == 0) {
			if (logger.isWarnEnabled()) {
				logger.warn("Cannot find a @StreamListener matching for message with id: "
						+ requestMessage.getHeaders().getId());
			}
			return null;
		}
		else if (matchingHandlers.size() > 1) {
			for (ConditionalStreamListenerMessageHandlerWrapper matchingMethod : matchingHandlers) {
				matchingMethod.getStreamListenerMessageHandler().handleMessage(requestMessage);
			}
			return null;
		}
		else {
			final ConditionalStreamListenerMessageHandlerWrapper singleMatchingHandler = matchingHandlers.get(0);
			singleMatchingHandler.getStreamListenerMessageHandler().handleMessage(requestMessage);
			return null;
		}
	}

	private List<ConditionalStreamListenerMessageHandlerWrapper> findMatchingHandlers(Message<?> message) {
		ArrayList<ConditionalStreamListenerMessageHandlerWrapper> matchingMethods = new ArrayList<>();
		for (ConditionalStreamListenerMessageHandlerWrapper conditionalStreamListenerMessageHandlerWrapperMethod : this.handlerMethods) {
			if (conditionalStreamListenerMessageHandlerWrapperMethod.getCondition() == null) {
				matchingMethods.add(conditionalStreamListenerMessageHandlerWrapperMethod);
			}
			else {
				boolean conditionMetOnMessage = conditionalStreamListenerMessageHandlerWrapperMethod.getCondition().getValue(
						this.evaluationContext, message, Boolean.class);
				if (conditionMetOnMessage) {
					matchingMethods.add(conditionalStreamListenerMessageHandlerWrapperMethod);
				}
			}
		}
		return matchingMethods;
	}

	static class ConditionalStreamListenerMessageHandlerWrapper {

		private final Expression condition;

		private final EventHandlerMessageHandler streamListenerMessageHandler;

		ConditionalStreamListenerMessageHandlerWrapper(Expression condition,
                                                       EventHandlerMessageHandler streamListenerMessageHandler) {
			Assert.notNull(streamListenerMessageHandler, "the message handler cannot be null");
			Assert.isTrue(condition == null || streamListenerMessageHandler.isVoid(),
					"cannot specify a condition and a return value at the same time");
			this.condition = condition;
			this.streamListenerMessageHandler = streamListenerMessageHandler;
		}

		public Expression getCondition() {
			return condition;
		}

		public boolean isVoid() {
			return this.streamListenerMessageHandler.isVoid();
		}

		public EventHandlerMessageHandler getStreamListenerMessageHandler() {
			return streamListenerMessageHandler;
		}
	}
}
