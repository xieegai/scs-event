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

package org.baihe.scs.event.config.adapter;

import org.baihe.scs.event.constant.EventBuiltinAttr;
import org.baihe.scs.event.lifecycle.IEventLifecycle;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.argument.StructuredArguments;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by baihe on 2017/4/7.
 */
@Slf4j
public class EventHandlerMethod extends InvocableHandlerMethod {

    private static final ThreadLocal<DateFormat> FMT_THREAD_LOCAL =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    private IEventLifecycle eventLifecycle;

    private Object myBean;

    private Method myMethod;

    private Boolean fallthrough = null;

    public EventHandlerMethod(Object bean, Method method, IEventLifecycle eventLifecycle) {
        super(bean, method);
        this.eventLifecycle = eventLifecycle;
        this.myBean = bean;
        this.myMethod = method;
    }

    private static String getStringFromHeader(MessageHeaders messageHeaders, String key) {
        Object value = messageHeaders.get(key);
        if (value == null) {
            return "";
        }
        if (String.class.isAssignableFrom(value.getClass())) {
            return (String)value;
        }
        if (value instanceof byte[]) {
            return new String((byte[])value);
        }
        throw new IllegalArgumentException("Incorrect type specified for header '" +
                key + "'. Expected [" + String.class + "] but actual type is [" + value.getClass() + "]");
    }

    private static Long getLongFromHeader(MessageHeaders messageHeaders, String key) {
        Object value = messageHeaders.get(key);
        if (Long.class.isAssignableFrom(value.getClass())) {
            return (Long)value;
        }
        if (value instanceof byte[]) {
            return Long.valueOf(new String((byte[])value));
        }
        throw new IllegalArgumentException("Incorrect type specified for header '" +
                key + "'. Expected [" + Long.class + "] but actual type is [" + value.getClass() + "]");
    }

    @Override
    public Object invoke(Message<?> message, Object... providedArgs) throws Exception {

//        if (fallthrough == null) {
//            fallthrough = !EventListenerCache.listenedOnMethod(myMethod);
//        }
//        if (fallthrough) {
//            return super.invoke(message, providedArgs);
//        }
        Long eventExecuteStart = System.currentTimeMillis();
        MessageHeaders messageHeaders = message.getHeaders();

        String eventSeq = getStringFromHeader(messageHeaders, EventBuiltinAttr.EVENT_ID.getKey());
        Class<?> eventPayloadClass = ClassUtils.resolveClassName(
                getStringFromHeader(messageHeaders, EventBuiltinAttr.EVENT_CLASS.getKey()), null);
        Long eventTriggerTime = getLongFromHeader(messageHeaders, EventBuiltinAttr.EVENT_TRIGGER_TIME.getKey());
        String producer = getStringFromHeader(messageHeaders, EventBuiltinAttr.EVENT_TRIGGER_APP.getKey());

        log.info("Received event {}, {}, {}, {}, {}, invoking {} ...",
                StructuredArguments.keyValue("eventId", eventSeq),
                StructuredArguments.keyValue("eventPayloadClass", eventPayloadClass.getName()),
                StructuredArguments.keyValue("triggerAt", FMT_THREAD_LOCAL.get().format(new Date(eventTriggerTime))),
                StructuredArguments.keyValue("producer", producer),
                StructuredArguments.keyValue("lagTime", checkDuration(eventTriggerTime, eventExecuteStart)),
                StructuredArguments.keyValue("method", myBean.getClass().getName() + "." + myMethod.getName()));

        Object eventResult = null;
        boolean needExec = eventLifecycle.onExecute(eventSeq, message.getPayload());
        if (needExec) {
            try {
                eventResult = super.invoke(message, providedArgs);
            } catch (Exception e) {
                log.error("Event handling failed! {}, {}, {}, {}",
                        StructuredArguments.keyValue("eventId", eventSeq),
                        StructuredArguments.keyValue("eventPayloadClass", eventPayloadClass.getName()),
                        StructuredArguments.keyValue("triggerAt", FMT_THREAD_LOCAL.get().format(new Date(eventTriggerTime))),
                        StructuredArguments.keyValue("producer", producer),
                        e);
                eventLifecycle.onFail(eventSeq, message.getPayload(), e);
                return null;
            }

            log.info("Event handling succeeded! {}, {}, {}, {}, {}",
                    StructuredArguments.keyValue("eventId", eventSeq),
                    StructuredArguments.keyValue("eventPayloadClass", eventPayloadClass.getName()),
                    StructuredArguments.keyValue("triggerAt", FMT_THREAD_LOCAL.get().format(new Date(eventTriggerTime))),
                    StructuredArguments.keyValue("producer", producer),
                    StructuredArguments.keyValue("executeTime", checkDuration(eventExecuteStart, System.currentTimeMillis())));
            eventLifecycle.onSucess(eventSeq, message.getPayload());
        }
        return eventResult;
    }

    private static String checkDuration(Long start, Long end) {
        Long msDiff = end - start;
        return msDiff < 1000 ? (msDiff + "ms") : ((msDiff/1000.0) + "s");
    }

}
