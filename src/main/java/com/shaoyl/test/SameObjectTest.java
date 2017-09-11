package com.shaoyl.test;

public class SameObjectTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	/*	String a = "good good test";
		test1(a);
		test2(a);
		System.out.println(a);*/
		
		User u = new User();
		u.setName("aaaa");
		u.setAge(20);
		
		tu1(u);
		tu2(u);
		System.out.println(u);
	}
	
	public static void test1(String b){
		b = b.substring(0,5);
		System.out.println(b);
	}
	public static void test2(String c){
		c = c.substring(3,5);
		System.out.println(c);
	}
	
	public static void tu1(User u){
		u.setName("bbbb");
		System.out.println(u.toString());
	}
	public static void tu2(User u){
		u.setName("ccc");
		System.out.println(u.toString());
	}
}
