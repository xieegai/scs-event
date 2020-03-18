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

package org.baihe.scs.event.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The enum to hold builtin event attributes
 * @author baihe
 * Created on 2019-11-20
 */
@AllArgsConstructor
public enum EventBuiltinAttr {
    EVENT_ID("XMEventId", "事件ID"),
    EVENT_CLASS("XMEventClass", "事件Class名称"),
    EVENT_TRIGGER_TIME("XMEventTriggerTime", "事件触发时间"),
    EVENT_TRIGGER_APP("XMEventTriggerApp", "事件触发的App");
    ;

    @Getter
    private String key;

    @Getter
    private String description;
}
