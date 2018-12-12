package com.revature;

import com.revature.Drivers.FemaleEducation;
import com.revature.Drivers.FemaleEmployment;
import com.revature.Drivers.FemaleGraduates;
import com.revature.Drivers.MaleEmployment;

/**
 * Hello world!
 */
public final class GenderStatistics {
    
    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
         
        int reduceTasks = new Integer(args[0]);
        switch (reduceTasks) {
            case 1:
                String[] input = {"GenderStatistics/data/Gender_StatsSeries.csv",
                                  "GenderStatistics/data/Gender_StatsData.csv",
                                  "GenderStatistics/outputGenderStats"
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
