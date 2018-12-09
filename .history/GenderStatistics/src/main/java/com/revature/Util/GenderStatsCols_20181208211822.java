package com.revature.Util;

/**
 * FemailEducationCols
 */
public class GenderStatsCols {

    
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
        final public static int[] YEAR = new int[LAST_YEAR - FIRST_YEAR];
    
        static{
           
            for (int i = 0,j=STARTING_YEAR_COLUMN_INDX; i < YEAR.length; i++,j++) {
                YEAR[i] =  j;
            }
        }
    }
    
}