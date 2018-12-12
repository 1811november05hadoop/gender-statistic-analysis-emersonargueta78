package com.revature;

import java.util.Map;
import java.util.Set;

import com.revature.Drivers.FemaleEducation;
import com.revature.Drivers.FemaleEmployment;
import com.revature.Drivers.FemaleGraduates;
import com.revature.Drivers.MaleEmployment;
import com.revature.Util.ReadFirstRow;

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
        Map<Integer,String> map = new ReadFirstRow().readFirstRow();
        Set<Integer> theset = map.keySet();
        
        for (Integer key : theset) {
            System.out.println("the key: "+key);
        }

        
        // int reduceTasks = new Integer(args[0]);
        // switch (reduceTasks) {
        //     case 1:
        //         String[] input = {"data/Gender_StatsSeries.csv",
        //                           "data/Gender_StatsData.csv",
        //                           "outputGenderStats"
        //                           };

        //         new FemaleEducation().run(input);    
        //         break;
        //     case 2:
        //         new FemaleGraduates();
        //         break;
        //     case 3:
        //         new FemaleEmployment();
        //         break;    
        //     case 4:
        //         new MaleEmployment();
        //         break;
        //     default:
        //         break;
        //}
    }
}
