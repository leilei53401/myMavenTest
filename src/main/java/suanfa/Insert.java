package suanfa;

public class Insert {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		int[] numbers = { 3, 2, 7, 8, 9, 6 };
		int[] numbers = { 6, 8, 5, 3, 9, 2 };

//		insertSort2(numbers);
		test3(numbers);
		printNumbers(numbers);

	}

	/**
	 * 插入排序
	 * 
	 * 从第一个元素开始，该元素可以认为已经被排序 取出下一个元素，
	 * 在已经排序的元素序列中从后向前扫描
	 * 如果该元素（已排序）大于新元素，将该元素移到下一位置 重复步骤3，
	 * 直到找到已排序的元素小于或者等于新元素的位置 将新元素插入到该位置中 重复步骤2
	 * 
	 * @param numbers
	 *            待排序数组
	 */
	public static void insertSort(int[] numbers) {
		int size = numbers.length;
		int temp = 0;
		int j = 0;

		for (int i = 0; i < size; i++) {
			temp = numbers[i];
			// 假如temp比前面的值小，则将前面的值后移
			for (j = i; j > 0 && temp < numbers[j - 1]; j--) {
				numbers[j] = numbers[j - 1];
			}
			numbers[j] = temp;
		}
	}
	
	
	public static void insertSort2(int[] numbers) {
		for (int i = 1; i < numbers.length; i++) {
			int j=i;
			int tmp = numbers[i];
			
			while(j>0 && tmp < numbers[j-1]){
				numbers[j] = numbers[j-1]; 
				j--;
			}
			numbers[j] =  tmp;
		}
	}
	
	public static void test3(int[] numbers){
		for(int i=1;i<numbers.length;i++){
			int temp = numbers[i];
			int j=i;
			
			while(j>0 && temp<numbers[j-1]){
				numbers[j] = numbers[j-1];
				j--;
			}
			numbers[j]=temp;
		}
	}
	
	
	public static void printNumbers(int [] numbers){
		for(int i:numbers){
			System.out.print(i+"\t");
		}
	}


}
