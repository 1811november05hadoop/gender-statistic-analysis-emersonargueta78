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
        String[] inputx = {args[1],args[2],args[3],args[4],args[5]};
        switch (job) {
            case 1:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -Identify the countries where
                 *  % of female graduates is less than 30%. 
                 */

                /**
                 * args[1] -- inputFile 1
                 * args[2] -- inputFile 2
                 * args[3] -- outputDirectory
                 * args[4] -- lowerYearRange
                 * args[5] -- upperYearRange
                 */
                
                new FemaleGraduates().run(inputx);
                break;
            case 2:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  - List the average increase in female
                 *  education in the U.S. from the year 2000.

                 */
                String[] input2 = {args[1],args[2],args[3],"2000","2016"};
                new FemaleEducation().run(input2);
                break;
            case 3:
                
                new FemaleEmployment().run(inputx);
                break;    
            case 4:
                new MaleEmployment().run(inputx);
                break;
            default:
                break;
        }
    }
}
