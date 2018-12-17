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
        
        int job = new Integer(args[0]);
        /**
         * args[1] -- inputFile 1
         * args[2] -- inputFile 2
         * args[3] -- outputDirectory
         * args[4] -- lowerYearRange
         * args[5] -- upperYearRange
        */
        String[] input1 = {args[1],args[2],args[3],args[4],args[5]};
        String[] inputx = {args[1],args[2],args[3],"2000","2016"};
        switch (job) {
            case 1:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -Identify the countries where
                 *  % of female graduates is less than 30%. 
                 */
                new FemaleGraduates().run(input1);
                break;
            case 2:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  - List the average increase in female
                 *  education in the U.S. from the year 2000.

                 */
                new FemaleEducation().run(inputx);
                break;
            case 3:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -List the % of change in male employment from the year 2000.
                 */
                new MaleEmployment().run(inputx);
                break;    
            case 4:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -List the % of change in female employment from the year 2000.
                 */
                new FemaleEmployment().run(inputx);
                break;
            default:
                break;
        }
    }
}
