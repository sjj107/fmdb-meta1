package com.fiberhome.fmdb.h2test;

import java.util.Random;
import java.util.UUID;

/**
 * Created by sjj on 19/10/22
 */
public class IdTest {
    public static void main(String[] args) {
        Random random = new Random();
        for(int i=0;i<10000;i++){
            System.out.println(System.currentTimeMillis()+String.format("%04d", random.nextInt(10000)));
        }
    }
}
