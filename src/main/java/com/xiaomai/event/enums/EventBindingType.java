package com.xiaomai.event.enums;

import java.lang.annotation.Annotation;

/**
 * @author baihe Created on 2020/5/22 10:05 AM
 */
public enum EventBindingType {
    INPUT,
    OUTPUT,
    BOTH,
    ;

    public static EventBindingType fromAnnotation(Class<? extends Annotation> qualifier) {
        return org.springframework.cloud.stream.annotation.Input.class.equals(qualifier) ?
            INPUT : OUTPUT;
    }
}
