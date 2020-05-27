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

package com.xiaomai.event.config.adapter;

import com.xiaomai.event.lifecycle.IEventLifecycle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.lang.reflect.Method;
import java.util.List;

public class EventHandlerMethodFactory extends DefaultMessageHandlerMethodFactory {

    @Value("${spring.application.name}")
    private String appName;

    private IEventLifecycle eventLifecycle;

    private final HandlerMethodArgumentResolverComposite myArgumentResolvers =
            new HandlerMethodArgumentResolverComposite();

    public EventHandlerMethodFactory(IEventLifecycle eventLifecycle) {
        this.eventLifecycle = eventLifecycle;
    }

    @Override
    public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
        EventHandlerMethod handlerMethod = new EventHandlerMethod(bean, method, this.eventLifecycle, appName);
        handlerMethod.setMessageMethodArgumentResolvers(this.myArgumentResolvers);
        return handlerMethod;
    }

    @Override
    public void setArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
        if (argumentResolvers == null) {
            this.myArgumentResolvers.clear();
            return;
        }
        this.myArgumentResolvers.addResolvers(argumentResolvers);
    }

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        if (this.myArgumentResolvers.getResolvers().isEmpty()) {
            this.myArgumentResolvers.addResolvers(initArgumentResolvers());
        }
    }
}
