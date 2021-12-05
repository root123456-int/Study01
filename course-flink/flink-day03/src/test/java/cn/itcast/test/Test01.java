package cn.itcast.test;

import java.util.Random;

/**
 * @author by FLX
 * @date 2021/7/19 0019 20:54.
 */
public class Test01 {
    public static void main(String[] args) {
        Random random = new Random();
        int i = random.nextInt(2) + 1;

        System.out.println(i);
    }
}
