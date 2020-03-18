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

package org.baihe.scs.event;

import org.baihe.scs.event.annotation.EventMeta;
import org.baihe.scs.event.constant.EventBuiltinAttr;
import org.baihe.scs.event.lifecycle.IEventLifecycle;
import org.baihe.scs.event.utils.EventBindingBeanDefinitionRegistryUtils;
import java.util.HashMap;
import java.util.Map;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * @author baihe Created on 2020/3/18 1:51 PM
 */
public class EventAgent<T> {

    private final Class<T> payloadClass;

    private final EventMeta eventMeta;

    private final String appName;

    private final IEventLifecycle eventLifecycle;

    private final BinderAwareChannelResolver resolver;

    protected EventAgent(Class<T> payloadClass, String appName, IEventLifecycle eventLifecycle, BinderAwareChannelResolver resolver) {
        this.appName = appName;
        this.eventLifecycle = eventLifecycle;
        this.resolver = resolver;
        this.payloadClass = payloadClass;
        this.eventMeta = payloadClass.getDeclaredAnnotation(EventMeta.class);
        Assert.state(null != this.eventMeta, "the specified payloadClass is not marked with the meta annotation");
    }

    /**
     * Trigger the event with payload
     * @param payload the event payload
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload) {
        return triggerEvent(payload, new HashMap<>());
    }

    /**
     * Trigger the event with payload and attributes
     * @param payload the event payload
     * @param eventAttrs the event attributes
     * @return the event sequence of the dispatched event
     */
    public String triggerEvent(T payload, Map<String, Object> eventAttrs) {
        String eventSeq = eventLifecycle.onTrigger(payload, eventAttrs);

        Map<String, Object> eventHeaders = new HashMap<String, Object>() {
            {
                put(EventBuiltinAttr.EVENT_ID.getKey(), eventSeq);
                put(EventBuiltinAttr.EVENT_CLASS.getKey(), payload.getClass().getName());
                put(EventBuiltinAttr.EVENT_TRIGGER_TIME.getKey(), System.currentTimeMillis());
                put(EventBuiltinAttr.EVENT_TRIGGER_APP.getKey(), appName);
            }
        };

        resolver.resolveDestination(EventBindingBeanDefinitionRegistryUtils.resolveOutputBindingName(payloadClass))
            .send(MessageBuilder.createMessage(payload,
                new MessageHeaders(eventHeaders)));
        return eventSeq;
    }
}
