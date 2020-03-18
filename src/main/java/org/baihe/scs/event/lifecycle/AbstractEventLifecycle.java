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

package org.baihe.scs.event.lifecycle;

import org.baihe.scs.event.annotation.EventMeta;
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

    public String genEventSeq(Object payload, Map<String, Object> eventAttrs) {
        EventMeta eventMeta = getEventMeta(payload);

        if (!eventMeta.idempotent())
            return UUID.randomUUID().toString();
        return makeRecord(eventMeta, payload, eventAttrs);
    }

    public abstract String makeRecord(EventMeta eventMeta, Object payload, Map<String, Object> eventAttrs);

    public boolean onExecute(String eventSeq, Object payload) {
        EventMeta eventMeta = getEventMeta(payload);
        super.onExecute(eventSeq, payload);

        if (!eventMeta.idempotent())
            return true;
        return preExecute(eventSeq, eventMeta, payload);
    }

    public abstract boolean preExecute(String eventSeq, EventMeta eventMeta, Object payload);

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
