package com.revature.map;

/**
 * Q2Mapper
 */
public class Q2Mapper {

    public static void main(String[] args) {
        Q1Mapper.yearLowerRange = "2000";
        Q1Mapper.yearUpperRange = "2010";

        System.out.println(new Q1Mapper().yearLowerRange);
        System.out.println(Q1Mapper.yearUpperRange);
    }
}