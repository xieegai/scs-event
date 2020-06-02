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

package com.xiaomai.event.lifecycle;

import com.google.common.collect.ImmutableList;
import com.xiaomai.event.annotation.EventMeta;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;
import java.util.UUID;

/**
 * Created by baihe on 2017/8/22.
 */
@Slf4j
public abstract class AbstractEventLifecycle extends DefaultEventLifecycle implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    public String makeRecord(EventMeta eventMeta, Object payload, Map<String, Object> eventAttrs, String channel) {
        return onRecord(eventMeta, payload, eventAttrs, channel);
    }

    public abstract String onRecord(EventMeta eventMeta, Object payload, Map<String, Object> eventAttrs, String channel);

    @Override
    public boolean onExecute(String eventSeq, String consumerKey, Class<?> payloadClass) {
        EventMeta eventMeta = getEventMeta(payloadClass);
        super.onExecute(eventSeq, consumerKey, payloadClass);

        if (!eventMeta.enableAudit())
            return true;
        if (eventMeta.consumerWhitelist().length > 0) {
            if (!ImmutableList.copyOf(eventMeta.consumerWhitelist()).contains(consumerKey)) {
                return false;
            }
        }
        return preExecute(eventSeq, consumerKey, eventMeta, payloadClass);
    }

    /**
     * pre the event consumed by consumer
     * @param eventSeq the event sequence
     * @param consumerKey the consumer key
     * @param eventMeta the event meta
     * @param payloadClass the event payload class
     * @return whether to execute
     */
    public abstract boolean preExecute(String eventSeq, String consumerKey, EventMeta eventMeta, Class<?> payloadClass);

    /**
     * Set the application context
     * @param applicationContext the application context
     * @throws BeansException
     */
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
