package org.liufree.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * @Author: ynz
 * @Date: 2019/1/23/023 10:45
 * @Version 1.0
 */
@Aspect
@Slf4j
@Component
public class CalTimeAspect {

    @Pointcut("@annotation(org.liufree.annotation.CalTime)")
    public void controller(){}

    @Around("controller()")
    public Object calTime(ProceedingJoinPoint joinPoint) throws Throwable {
        Long startTime = System.currentTimeMillis();
        Object o = joinPoint.proceed();
        log.info(String.format("runtime:%s ms",(System.currentTimeMillis()-startTime)));
        if(o instanceof String){
            String resultString = String.valueOf(o)+","+String.format("runtime:%s ms",(System.currentTimeMillis()-startTime));
            return resultString;
        }
        return o;

    }
}
