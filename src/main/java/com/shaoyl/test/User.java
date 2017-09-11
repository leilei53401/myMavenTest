package com.shaoyl.test;

public class User {
String name;
int age;
String sex;
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public int getAge() {
	return age;
}
public void setAge(int age) {
	this.age = age;
}
public String getSex() {
	return sex;
}
public void setSex(String sex) {
	this.sex = sex;
}
@Override
public String toString() {
	return "User [name=" + name + ", age=" + age + ", sex=" + sex + "]";
}

}
