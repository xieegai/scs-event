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

package com.xiaomai.event.lifecycle;

import com.xiaomai.event.utils.StructuredArguments;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Created by baihe on 2017/8/23.
 */
@Slf4j
public class DefaultEventLifecycle implements IEventLifecycle {

    public String genEventSeq(Object payload, Map<String, Object> eventAttrs) {
        return System.currentTimeMillis() + "";
    }

    @Override
    public String onTrigger(Object payload, Map<String, Object> eventAttrs) {
        String eventSeq = genEventSeq(payload, eventAttrs);
        log.info("Mark event {}, {} pending",
                StructuredArguments.keyValue("eventId", eventSeq),
                StructuredArguments.keyValue("eventPayloadClass", payload.getClass().getName()));
        return eventSeq;
    }

    @Override
    public boolean onExecute(String eventSeq, Class<?> payloadClass) {
        log.info("Mark event {}, {} executing",
                StructuredArguments.keyValue("eventId", eventSeq),
                StructuredArguments.keyValue("eventPayloadClass", payloadClass.getName()));
        return true;
    }

    @Override
    public void onSucess(String eventSeq, Class<?> payloadClass) {
        log.info("Mark event {}, {} committed",
                StructuredArguments.keyValue("eventId", eventSeq),
                StructuredArguments.keyValue("eventPayloadClass", payloadClass.getName()));
    }

    @Override
    public void onFail(String eventSeq, Class<?> payloadClass, Exception e) {
        log.info("Mark event {}, {} failed, {}",
                StructuredArguments.keyValue("eventId", eventSeq),
                StructuredArguments.keyValue("eventPayloadClass", payloadClass.getName()),
                StructuredArguments.keyValue("reason", e.getMessage()));
    }


}
