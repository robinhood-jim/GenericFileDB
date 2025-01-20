package com.robin.gfdb;

import com.robin.msf.bean.ApplicationContextHolder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;


public class ServerApplication {
    public static void main(String[] args){
        ApplicationContext context= Micronaut.run(ServerApplication.class);
        ApplicationContextHolder.setContext(context);
    }
}
