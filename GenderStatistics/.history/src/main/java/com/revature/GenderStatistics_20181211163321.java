package com.revature;

import com.revature.Drivers.FemaleEducation;
import com.revature.Drivers.FemaleEmployment;
import com.revature.Drivers.FemaleGraduates;
import com.revature.Drivers.MaleEmployment;

/**
 * Hello world!
 */
public final class GenderStatistics {
    
    public static int lowerYearRange;
    public static int upperYearRange;
    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        
        /**
         * args 0: is the map and reducer task to run
         * args 1: is the lower year range to select data from
         * args 2: is the upper year range to select data from
         */
        int reduceTasks = new Integer(args[0]);
        switch (reduceTasks) {
            case 1:
                String[] input = {"data/Gender_StatsSeries.csv",
                                  "data/Gender_StatsData.csv",
                                  "outputGenderStats",
                                  args[1],
                                  args[2]
                                  };

                new FemaleEducation().run(input);    
                break;
            case 2:
                new FemaleGraduates();
                break;
            case 3:
                new FemaleEmployment();
                break;    
            case 4:
                new MaleEmployment();
                break;
            default:
                break;
        }
    }
}
