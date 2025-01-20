package com.robin.msf.bean;

import cn.hutool.core.lang.Assert;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.Qualifier;

public class ApplicationContextHolder {
    private static ApplicationContext context;
    private ApplicationContextHolder(){

    }
    public static void setContext(ApplicationContext context1){
        context=context1;
    }
    public static <T> T getBean(Class<T> clazz){
        Assert.notNull(context);
        return context.getBean(clazz);
    }
    public static <T> T getBean(Class<T> clazz, Qualifier<T> qualifier){
        Assert.notNull(context);
        return context.getBean(clazz,qualifier);
    }
}
