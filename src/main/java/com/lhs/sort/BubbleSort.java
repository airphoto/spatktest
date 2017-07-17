package com.lhs.sort;

/**
 * 冒泡排序
 * 冒泡排序的基本思想是：设排序序列的记录个数为n，进行n-1次遍历，每次遍历从开始位置依次往后比较前后相邻元素，这样较大的元素往后移，n-1次遍历结束后，序列有序。
 * 时间复杂度 n^2
 *
 */
public class BubbleSort {
    public static void main(String[] args) {
        int[] array = new int[]{1,26,3,2,9,10,4};
        int[] t = bubbleSort(array);
        for (int i = 0 ; i< t.length;i++){
            System.out.println(t[i]);
        }
    }

    static int[] bubbleSort(int[] array){
        boolean flag = true;
        for (int i=0; i< array.length -1 && flag ; i++){
            flag = false;
            for(int j=0;j<array.length-1-i;j++){
                if(array[j] > array[j+1]){
                    int tmp = array[j];
                    array[j] = array[j+1];
                    array[j+1] = tmp;
                    flag = true;
                }
            }
        }
        return array;
    }
}
