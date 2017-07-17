package com.lhs.search;

/**
 * 二分法查找
 * 时间负载读为 logn
 */
public class BinarySearch {

    public static void main(String[] args) {

    }

    static int binarySearch(int[] array,int data){
        int low = 0;
        int high = array.length -1;
        while (low<=high){
            int mid = (low+high) / 2;
            if(data == array[mid]){
                return mid;
            }else if(data< array[mid]){
                high = mid-1;
            }else{
                low = mid + 1;
            }
        }
        return -1;
    }
}
