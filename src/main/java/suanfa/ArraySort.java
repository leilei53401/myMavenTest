package suanfa;

public class ArraySort {

	   //两个有序数组的合并函数
	   public static int[] MergeList(int a[],int b[])
	   {
	       int result[];  
	       if(checkSort(a) && checkSort(b))  //检查传入的数组是否是有序的
	       {
	           result = new int[a.length+b.length];
	           
	           int i=0,j=0,k=0;   //i:用于标示a数组    j：用来标示b数组  k：用来标示传入的数组

	           while(i<a.length && j<b.length)
	               if(a[i] <= b[j]) {
	                   result[k++] = a[i++];
	               }else{
	                   result[k++] = b[j++];
	               }

	           /* 后面连个while循环是用来保证两个数组比较完之后剩下的一个数组里的元素能顺利传入 */
	           while(i < a.length) 
	               result[k++] = a[i++];
	           while(j < b.length)
	               result[k++] = b[j++];
	           
	           return result;
	       }
	       else 
	       {
	           System.out.print("非有序数组，不可排序！");
	           return null;
	       }
	   }
	   
	   //检查数组是否是顺序存储的
	   public static boolean checkSort(int a[])
	   {
	       boolean change = true;  //这个标志位是一种优化程序的方法，可以看看我写的冒泡排序优化就会明白了
	       for(int i=0; i<a.length-1 && change; i++)
	       {
	           for(int j=i+1; j<a.length; j++)
	               if(a[j-1] > a[j])
	                   return false;
	               else
	            	   change = false;
	       }
	       return true;        
	   }
	   
	   // 打印函数
	   public static void print(int b[])
	   {
	        for(int i=0; i<b.length ; i++)
	        {
	            System.out.print(b[i] + (i%10 ==9 ? "\n":"\t"));
	        }
	   }
	   
	   public static void main(String args[])
	   {
	       int a[]={1,2,2,3,5,6,7,7};
	       int b[]={1,2,4,5,8,8,9,10,11,12,12,13,14};
	       int c[]= MergeList(a,b);
	       if(c!=null)
	       print(c);
	       else
	           System.out.println("");
	   }

}
