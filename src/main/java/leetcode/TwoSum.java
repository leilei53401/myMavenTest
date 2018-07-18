package leetcode;
/**
 * <pre>
 * Given an array of integers, find two numbers such that they add up to a specific target number.
 * The function twoSum should return indices of the two numbers such that they add up to the target,
 * where index1 must be less than index2. Please note that your returned answers (both index1 and index2)
 * are not zero-based.
 * You may assume that each input would have exactly one solution.
 *
 * Input: numbers={2, 7, 11, 15}, target=9
 * Output: index1=1, index2=2
 *
 * 题目大意
 * 给定一个整数数组，找出其中两个数满足相加等于你指定的目标数字。
 * 要求：这个函数twoSum必须要返回能够相加等于目标数字的两个数的索引，且index1必须要小于index2。
 * 请注意一点，你返回的结果（包括index1和index2）都不是基于0开始的。你可以假设每一个输入肯定只有一个结果。
 *
 * 解题思路
 * 创建一个辅助类数组，对辅助类进行排序，使用两个指针，开始时分别指向数组的两端，看这两个下标对应的值是否
 * 等于目标值，如果等于就从辅助类中找出记录的下标，构造好返回结果，返回。如果大于就让右边的下标向左移，
 * 进入下一次匹配，如果小于就让左边的下标向右移动，进入下一次匹配，直到所有的数据都处理完
 * </pre>
 *

 * @return  3 , 4
 */
public class TwoSum {

    int [] array = {3,4,5,9};
    int target = 14;

    public static void main(String[] args){
        TwoSum twoSum = new TwoSum();
        int[] resut = twoSum.twoSum();

        System.out.println(resut);

        System.out.println(resut[0]);
        System.out.println(resut[1]);
    }


    public int [] twoSum(){
        int[] resut= new int[2];

        int starIndex = 0;
        int endIndex = array.length-1;

        boolean find = false;

        for(int i=starIndex;i<=endIndex;i++){
            int startValue = array[i];

            for(int j=endIndex ; j>i;j--){
                int endValue = array[j];
                if(startValue+endValue == target){
                    find = true;
                    resut[0] = i+1;
                    resut[1] = j+1;
                    break;
                }

            }

            if(find) break;

        }


        return resut;

    }

}
