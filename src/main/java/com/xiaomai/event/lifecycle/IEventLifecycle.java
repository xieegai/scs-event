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



import com.xiaomai.event.annotation.EventMeta;
import java.util.Map;

/**
 * Created by baihe on 2017/8/22.
 */
public interface IEventLifecycle {

    default EventMeta getEventMeta(Class<?> payloadClass) {
        return payloadClass.getDeclaredAnnotation(EventMeta.class);
    }

    /**
     * 当生产者开始生产事件时调用
     * @param payload 事件payload信息
     * @param eventAttrs 事件属性
     * @return 事件序列号
     */
    default String onIssue(Object payload, Map<String, Object> eventAttrs) {
        return onIssue(null, payload, eventAttrs);
    }

    /**
     * 当生产者开始生产事件时调用
     * @param eventSeq 事件序列号
     * @param payload 事件payload信息
     * @param eventAttrs 事件属性
     * @return 事件序列号
     */
    String onIssue(String eventSeq, Object payload, Map<String, Object> eventAttrs);

    /**
     * 当消费者开始消费时调用
     * @param eventSeq 事件序列号
     * @param consumerKey 消费者标识
     * @param payloadClass 消费事件类
     * @return 是否需要执行
     */
    boolean onExecute(String eventSeq, String consumerKey, Class<?> payloadClass);

    /**
     * 当消费者消费成功时调用
     * @param eventSeq 事件序列号
     * @param consumerKey 消费者标识
     * @param payloadClass 消费事件类
     */
    void onSuccess(String eventSeq, String consumerKey, Class<?> payloadClass);

    /**
     * 当消费者消费失败时调用
     * @param eventSeq 事件序列号
     * @param consumerKey 消费者标识
     * @param payloadClass 消费事件类
     * @param e 消费失败异常信息
     */
    void onFail(String eventSeq, String consumerKey, Class<?> payloadClass, Exception e);
}
