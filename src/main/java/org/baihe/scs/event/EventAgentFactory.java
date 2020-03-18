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

import org.baihe.scs.event.lifecycle.IEventLifecycle;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;

/**
 * The agent bean to trigger event
 *
 * @author baihe
 * Created on 2020-03-13 23:03
 */
@Slf4j
public class EventAgentFactory {

    /**
     * The application name to mark the event attributes
     */
    @Value("${spring.application.name:application}")
    private String appName;

    /**
     * The event lifecycle instance to trace the event procession
     */
    private IEventLifecycle eventLifecycle;

    /**
     * The binder resolver to dispatch the event payload
     */
    private BinderAwareChannelResolver resolver;

    private static final Map<Class<?>, EventAgent> agentMap = new ConcurrentHashMap<>();

    /**
     * CONSTRUCTOR
     * @param eventLifecycle the provided event lifecycle instance
     * @param resolver the provided binder resolver
     */
    public EventAgentFactory(IEventLifecycle eventLifecycle, BinderAwareChannelResolver resolver) {
        this.eventLifecycle = eventLifecycle;
        this.resolver = resolver;
    }

    /**
     * Retrieve the EventAgent object by event payload class
     * @param payloadClass the event payload class
     * @return the EventAgent object of payload class
     */
    public static <T> EventAgent<T> of(Class<T> payloadClass) {
        EventAgent<T> eventAgent = (EventAgent<T>)agentMap.computeIfAbsent(payloadClass, pclass -> {
            EventAgent<T> agent = new EventAgent<>(payloadClass, INSTANCE.appName,
                INSTANCE.eventLifecycle, INSTANCE.resolver);
            return agent;
        });
        return eventAgent;
    }

    public static EventAgentFactory INSTANCE;
}


