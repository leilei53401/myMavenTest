package suanfa;

/**
 * Created by shaoyl on 2018-2-26.
 */
public class QuikSortMy1 {
    public static void main(String[] args){
         int [] intArray = {10,13,11,20,18,3,5,8,1,15,4,17,2,9,7};
        sort(intArray,0,intArray.length-1);
        printNumbers(intArray);
        // 注：比此方法更优，更好理解的参考 QuickSort.java
        //省去了交换的步骤，请详细比较理解。

    }

    public static void sort(int[] array, int left, int right){
        if(left >= right){
            return;
        }
        int index = parrtition(array,left,right); //注：此处第二个，第三个参数分别是传进来的 left 和 right ，而不能直接设置为 0 和 array.length-1
        sort(array,left,index-1);   //注：第二个参数为 left ， 而不能直接设置为 0
        sort(array,index+1,right); //注： 第三个参数 为传递过来的参数right ，而不能是 array.length-1
    }

    public static int parrtition(int[] array,int left, int right){
        int key = array[left];
        int start =  left;
        while (left != right){
            //从右向左扫描
            while (array[right] >= key && right > left) {
                right--;
            }
            //从左向右扫描
            while (array[left] <= key && right > left) {
                left++;
            }

            if(left<right){
                //将左侧第一个大于key的数据，右侧第一个小于key的数据，交换位置
                int tmp =  array[right];
                array[right] =  array[left];
                array[left] = tmp;
            }
        }

        //左右指向同一位置，且这个数肯定小于 key ，(原因是优先从右向左扫描)
        //退出while循环，并交换此位置数据与key的数据
        array[start] = array[left];
        array[left] = key;
        return left;
    }



    public static void printNumbers(int [] numbers){
        for(int i:numbers){
            System.out.print(i+"\t");
        }
    }
}
