package test;

public class TargetNumber 
{
public static void main(String[] args)
{
 
   int targetNumber=1; 
   int count=0;
   boolean condition=false;
   int[] numbers=new int[10];
   
   while (count!=10){
	targetNumber++;
	condition=(targetNumber%2==1)&&(targetNumber%3==0)
			&&(targetNumber%4==1)&&(targetNumber%5==4)
			&&(targetNumber%6==3)&&(targetNumber%7==0)
			&&(targetNumber%8==1)&&(targetNumber%9==0);
	if (condition){
		count++;
		numbers[count-1]=targetNumber;
	}
}
   System.out.println("Target Number are:");
   for (int i=1;i<=numbers.length;i++){
   System.out.println(numbers[i-1]);} 
} 
} 
