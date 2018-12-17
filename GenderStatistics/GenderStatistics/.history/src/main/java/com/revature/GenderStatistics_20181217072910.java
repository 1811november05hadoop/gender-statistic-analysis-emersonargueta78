package com.revature;

import com.revature.driver.DriverQ1;
import com.revature.driver.DriverQ2;
import com.revature.driver.DriverQ3;
import com.revature.driver.DriverQ4;

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
         * args[2] -- outputDirectory
         * args[3] -- lowerYearRange
         * args[4] -- upperYearRange
        */
        String[] inputx = {args[1],args[2],args[3],args[4]};
        switch (job) {
            case 1:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -Identify the countries where
                 *  % of female graduates is less than 30%. 
                 */
                
                new DriverQ1().run(inputx);
                break;
            case 2:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  - List the average increase in female
                 *  education in the U.S. from the year 2000.

                 */
                new DriverQ2().run(inputx);
                break;
            case 3:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -List the % of change in male employment from the year 2000.
                 */
                new DriverQ3().run(inputx);
                break;    
            case 4:
                /**
                 * Runs the map and reduce task to answer
                 * business question:
                 *  -List the % of change in female employment from the year 2000.
                 */
                new DriverQ4().run(inputx);
                break;
            default:
                break;
        }
    }
}
