package com.revature.Util;

import java.util.ArrayList;
import java.util.Map.Entry;

/**
 * FemailEducationCols
 */
public class GenderStatsCols {

    public static class Series{
        final private static String COL_NAMES_STR = ReadFirstLine.readLine("/GenderStatistics/data/Gender_StatsSeries.csv");
        final private static Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(COL_NAMES_STR);
        final public static ArrayList<String> COL_NAMES = entry.getValue();
        final public static int SERIES_CODE = 0;
        final public static int INDICATOR_NAME = 2;
        

    }

    public static class Data{
    
        final private static int LAST_YEAR = 2016;
        final private static int FIRST_YEAR = 1960;
        final private static int STARTING_YEAR_COLUMN_INDX = 4;
    
        /**
         * This is the index for the country column value
         * in a list containing the column values of each row 
         * of gender statistics data
         * 
        **/    
        final public static int COUNTRY = 0;
        
        /**
         * This is the indexes for the years for the data
         * 
         */
        final public static int[] YEAR = new int[LAST_YEAR - FIRST_YEAR];
    
        static{
           
            for (int i = 0,j=STARTING_YEAR_COLUMN_INDX; i < YEAR.length; i++,j++) {
                YEAR[i] =  j;
            }
        }
    }
    
}