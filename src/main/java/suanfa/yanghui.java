package suanfa;

public class yanghui {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 System.out.println("请输入行数：");
//	        Scanner sc = new Scanner(System.in);
	        int line = 10;//sc.nextInt();
	        int[] a = new int[line];
	        for (int i = 0; i < line; i++) {
	            a[i] = 1;
	        }
	        if (line == 1) {
	            System.out.println(1);
	        } else if (line == 2) {
	            System.out.println(1);
	            System.out.println(1 + "\t" + 1);
	        } else {
	            System.out.println(1);
	            System.out.println(1 + "\t" + 1);
	            for (int i = 1; i < line-1; i++) {
	                for (int j = i; j >= 1; j--) {
	                    a[j] = a[j] + a[j - 1];
	                }
	                for(int k =0;k<i+2;k++){
	                    System.out.print(a[k]+"\t");
	                }
	                System.out.println();
	            }
	        }
	}

}
