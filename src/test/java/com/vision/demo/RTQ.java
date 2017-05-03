package com.vision.demo;

public class RTQ {
	public static void main(String[] args) throws InterruptedException {
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				super.run();
				System.out.println("FFFFFF");
			}
			
		});
		Thread.sleep(1000000000000L);
	}
}
