package com.revature;

import org.apache.log4j.Logger;

/**
 * Main
 */
public class Main {
    final private static Logger LOGGER =  Logger.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        switch(Integer.valueOf(args[0])){
            case 5:
                LOGGER.info("Starting job 5");
                // String[] commandFemaleGraduation = {};
                // new GenderStatistics().run(commandFemaleGraduation);        
            case 4:
                LOGGER.info("Starting job 4");
                // String[] commandFemaleEducation = {};
                // new GenderStatistics().run(commandFemaleEducation);        
            case 3:
                LOGGER.info("Starting job 3");
                // String[] commandMaleEmployment = {};
                // new GenderStatistics().run(commandMaleEmployment);        
            case 2:
                LOGGER.info("Starting job 2");
                String[] commandFemaleEmployment = {
                                                    "1",
                                                    "data/Gender_StatsSeries.csv",
                                                    "data/Gender_StatsData.csv",
                                                    "outputGenderStats/BQ2",
                                                   };
                new GenderStatistics().run(commandFemaleEmployment);        
            case 1:
                LOGGER.info("Starting job 1");
                String[] commandEmploymentEducationRelationship = {
                                                                    "1",
                                                                    "data/Gender_StatsSeries.csv",
                                                                    "data/Gender_StatsData.csv",
                                                                    "outputGenderStats/BQ1",
                                                                    args[1],
                                                                    args[2]
                                                                  };
                new GenderStatistics().run(commandEmploymentEducationRelationship);        
                break;
            
        }

    }
}