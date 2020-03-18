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

package org.baihe.scs.event.config;

import org.springframework.cloud.stream.binding.AbstractBindingTargetFactory;
import org.springframework.cloud.stream.binding.MessageConverterConfigurer;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.SubscribableChannel;

public class EventChannelBindingTargetFactory extends AbstractBindingTargetFactory {

    private MessageConverterConfigurer messageConverterConfigurer;

    public EventChannelBindingTargetFactory(EventBindingServiceProperties eventBindingServiceProperties, CompositeMessageConverterFactory compositeMessageConverterFactory) {
        super(SubscribableChannel.class);
        this.messageConverterConfigurer = new MessageConverterConfigurer(eventBindingServiceProperties, compositeMessageConverterFactory);
    }

    @Override
    public Object createInput(String name) {
        SubscribableChannel subscribableChannel = new DirectChannel();
        this.messageConverterConfigurer.configureInputChannel(subscribableChannel, name);
        return subscribableChannel;
    }

    @Override
    public Object createOutput(String name) {
        SubscribableChannel subscribableChannel = new DirectChannel();
        this.messageConverterConfigurer.configureOutputChannel(subscribableChannel, name);
        return subscribableChannel;
    }
}
