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

package com.xiaomai.event.annotation;

import com.xiaomai.event.config.EventAgentConfiguration;
import com.xiaomai.event.config.EventBindingBeansRegistrar;
import com.xiaomai.event.config.EventBindingConfiguration;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Annotation for event handler provider class
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@EnableBinding
@Import({EventBindingBeansRegistrar.class,
    EventAgentConfiguration.class, EventBindingConfiguration.class})
public @interface EnableEventBinding {
    EventProducer[] produce() default {};
    Class<?>[] listenerClass() default {};
}
