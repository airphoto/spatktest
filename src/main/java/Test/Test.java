package Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/9/27.
 */
public class Test {
//    public static void main(String[] args) {
//        System.out.println(hash("key"));
//    }
//
//    public static int hash(Object k) {
//        int h = 0;
//        if (0 != h && k instanceof String) {
//            return sun.misc.Hashing.stringHash32((String) k);
//        }
//        int has = k.hashCode(); //计算hashCode
//        h ^= has; //异或操作
//        System.out.println(Integer.toBinaryString(h)+"->"+h);
//
//        // This function ensures that hashCodes that differ only by
//        // constant multiples at each bit position have a bounded
//        // number of collisions (approximately 8 at default load factor).
//
//        //hashCode长度为32位
//        int h20 = h >>> 20; //无符号向右异动20位
//        System.out.println(Integer.toBinaryString(h20)+"->"+h20);
//        int h12 = h >>> 12; //无符号向右异动12位
//        System.out.println(Integer.toBinaryString(h12)+"->"+h12);
//        h ^= h20 ^ h12;     //异或操作
//        System.out.println(Integer.toBinaryString(h)+"->"+h);
//        int h7 = h >>> 7;
//        System.out.println(Integer.toBinaryString(h7)+"->"+h7);
//        int h4 = h >>> 4;
//        System.out.println(Integer.toBinaryString(h4)+"->"+h4);
//        int hResult = h ^ h7 ^ h4;
//        System.out.println(Integer.toBinaryString(hResult)+"->"+hResult);
//        return hResult;
//    }
}
