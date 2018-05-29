package com.datastax.dse.java.aop;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Component;

//@Aspect

public class LoggingAspects {	

	
	@Before("execution(* com.datastax.dse.java..*.*(..))")
	public void allServiceMethodsAdvice(JoinPoint joinPoint){
		System.out.println("logBefore() is running!");
		System.out.println("hijacked : " + joinPoint.getSignature().getName());
		System.out.println("******");	}
}
