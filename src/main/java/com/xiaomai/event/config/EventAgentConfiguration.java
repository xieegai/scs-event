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

package com.xiaomai.event.config;

import com.xiaomai.event.kafka.BinderPartitionSelector;
import com.xiaomai.event.lifecycle.DefaultEventLifecycle;
import com.xiaomai.event.lifecycle.IEventLifecycle;
import com.xiaomai.event.EventAgentFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author baihe
 * date: 2017/11/24
 */
@Configuration
@Slf4j
public class EventAgentConfiguration {
    @Bean
    public EventAgentFactory eventAgentFactory(IEventLifecycle eventLifecycle, BinderAwareChannelResolver resolver) {
        EventAgentFactory agentFactory = new EventAgentFactory(eventLifecycle, resolver);
        EventAgentFactory.setInstacne(agentFactory);
        return agentFactory;
    }

    @Bean
    @ConditionalOnMissingBean
    public IEventLifecycle eventLifecycle() {
        log.warn("*NO* bean of IEventLifecycle provided, use {} ...", DefaultEventLifecycle.class.getName());
        return new DefaultEventLifecycle();
    }

    @Bean(name = "binderPartitionSelector")
    public BinderPartitionSelector binderPartitionSelector() {
        return new BinderPartitionSelector();
    }
}
