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
    public void run(String[] args) throws Exception {
        
        /**
         * args 0: is the map and reducer task to run
         * args 1: is the lower year range to select data from
         * args 2: is the upper year range to select data from
         */
        int job = new Integer(args[0]);
        switch (job) {
            case 1:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -Identify the countries where
                 *  % of female graduates is less than 30%. 
                 */
                String[] input0 = {args[1],args[2],args[3],args[4],args[5]};
                new FemaleGraduates().run(input0);
                break;
            case 2:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  - List the average increase in female
                 *  education in the U.S. from the year 2000.

                 */
                String[] input1 = {args[1],args[2],args[3],args[4],args[5]};
                new FemaleEducation();
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
