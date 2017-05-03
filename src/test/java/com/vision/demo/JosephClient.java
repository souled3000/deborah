package com.vision.demo;

import backtype.storm.utils.DRPCClient;

public class JosephClient {

	public static void main(String[] args) throws Exception {
		DRPCClient client = new DRPCClient("193.168.1.115", 3772);
		// for (int i = 0; i < 60; i++) {
		for (;;) {
			String rt = client.execute("joseph", "ALL 1 2 3 4 5 6 7 8 9");
			// System.out.println(i+" DRPC RESULT: " +rt);
			System.out.println(" DRPC RESULT: " + rt);

			Thread.sleep(1000);
		}
	}

}
