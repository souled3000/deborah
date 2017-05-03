package com.vision.main;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Deborah {

	private static final Logger log = LoggerFactory.getLogger(Deborah.class);
	private static ApplicationContext ctx;

	public static void main(String[] args)  {
		String prefix = "";
		List<String> cfg = new ArrayList<String>();
		cfg.add(prefix + "beans.xml");
		cfg.add(prefix + "quartz.xml");
		cfg.add(prefix + "e1.xml");
		
		ctx = new ClassPathXmlApplicationContext(cfg.toArray(new String[]{}));
		
		log.debug(ctx.getApplicationName());
		
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			
			public void uncaughtException(Thread t, Throwable e) {
				log.error("",e);
			}
		});
	}

}
