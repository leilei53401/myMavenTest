package com.voole.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.joda.time.DateTime;

public class BlockQueueTest {

	public static void main(String[] s){
//		arrayBlockQueueTest();
		linkedBlockQueueTest();
	}

	public static void arrayBlockQueueTest() {
		final int queue_max = 20000;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(queue_max);
		Thread thread_pro = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true){
						for(int i=0;i<50000;i++){
							queue.put(i+"");
							if(queue.size()>=queue_max){
								System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+"==============="+ queue.size());
							}
						}
						Thread.currentThread().sleep(1000*60*60);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		Thread thread_con = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true){
						queue.take();
						Thread.currentThread().sleep(300);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		thread_pro.start();
		thread_con.start();
	}
	public static void linkedBlockQueueTest() {
		final int queue_max = 20000;
		final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();
		Thread thread_pro = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true){
						for(int i=0;i<50000;i++){
							queue.put(i+"");
							if(queue.size()>=queue_max){
								System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss") + "==============="+ queue.size());
							}
						}
						Thread.currentThread().sleep(1000*60*60);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		Thread thread_con = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(true){
						queue.take();
						Thread.currentThread().sleep(300);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		thread_pro.start();
		thread_con.start();
	}
}
