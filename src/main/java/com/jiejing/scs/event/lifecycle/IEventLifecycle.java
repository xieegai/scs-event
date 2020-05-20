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

package com.jiejing.scs.event.lifecycle;



import com.jiejing.scs.event.annotation.EventMeta;
import java.util.Map;

/**
 * Created by baihe on 2017/8/22.
 */
public interface IEventLifecycle {

    default EventMeta getEventMeta(Class<?> payloadClass) {
        return payloadClass.getDeclaredAnnotation(EventMeta.class);
    }

    String onTrigger(Object payload, Map<String, Object> eventAttrs);

    boolean onExecute(String eventSeq, Class<?> payloadClass);

    void onSucess(String eventSeq, Class<?> payloadClass);

    void onFail(String eventSeq, Class<?> payloadClass, Exception e);
}
