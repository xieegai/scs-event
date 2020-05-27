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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;

/**
 * The event define annotation to mark some meta info on event payload class
 *
 * note: we wanna support partition-strategy param here at first. But finally
 * we found it can be based on the hash-code of payload instance. If you want
 * to adjust the dispatch partition-strategy of event payload, you should
 * override the hashcode function of the payload class, e.g., using the Lombok
 * {@link lombok.EqualsAndHashCode} Annotation.
 *
 * @author baihe
 * Created on 2020-03-20
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EventMeta {

	/**
	 * The name of the event
	 */
	String name();

	/**
	 * The description of the event
	 */
	String description();

	/**
	 * The domain of the event, if not blank, the event key will be as <domain>.<name>
	 */
	String domain() default "";

	/**
	 * The partition key based on attributes
	 */
	String[] partitionOn() default {};

	/**
	 * Whether or not audit the event lifecycle
	 */
	boolean enableAudit() default false;

	/**
	 * Whether the event is dedicated to specified consumers
	 */
	String[] consumerWhitelist() default {};

}
