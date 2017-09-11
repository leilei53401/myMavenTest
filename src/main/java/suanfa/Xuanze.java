package suanfa;

public class Xuanze {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int [] numbers = {3,2,7,8,9,6};
		
		selectSort2(numbers);
		printNumbers(numbers);
	}

	/**
	 * 选择排序算法 在未排序序列中找到最小元素，
	 * 存放到排序序列的起始位置 再从剩余未排序元素中继续寻找最小元素，
	 * 然后放到排序序列末尾。
	 * 以此类推，直到所有元素均排序完毕。
	 * 
	 * @param numbers
	 */
	public static void selectSort(int[] numbers) {
		int size = numbers.length; // 数组长度
		int temp = 0; // 中间变量

		for (int i = 0; i < size; i++) {
			int minIndex = i; // 待确定的位置
			// 选择出应该在第i个位置的数
			for (int j = size - 1; j > i; j--) {
				if (numbers[j] < numbers[minIndex]) {
					minIndex = j;
				}
			}
			// 交换两个数
			temp = numbers[i];
			numbers[i] = numbers[minIndex];
			numbers[minIndex] = temp;
		}
	}
	
	public static void selectSort2(int[] numbers) {
		int tmp=0;
		for(int i=0;i<numbers.length-1;i++){
			int minIndex = i;
			for(int j=i+1;j<numbers.length;j++){
				if(numbers[j]<numbers[minIndex]){
					tmp = numbers[j];
					numbers[j] = numbers[minIndex];
					numbers[minIndex] = tmp;
				}
			}
			
		}
	}
	
	public static void printNumbers(int [] numbers){
		for(int i:numbers){
			System.out.print(i+"\t");
		}
	}

}
