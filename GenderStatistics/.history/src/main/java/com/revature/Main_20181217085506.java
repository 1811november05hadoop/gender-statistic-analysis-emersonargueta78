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
                String[] commandMaleEducation = {
                    "5",
                    "data/Gender_StatsData.csv",
                    "outputGenderStats/BQ5",
                    "2000",
                    "2016"
                };
                new GenderStatistics().run(commandMaleEducation);        
                break;
            case 4:
                LOGGER.info("Starting job 4");
                String[] commandFemaleEmployment = {
                    "4",
                    "data/Gender_StatsData.csv",
                    "outputGenderStats/BQ4",
                    "2000",
                    "2016"
                };
                new GenderStatistics().run(commandFemaleEmployment);
                break;        
            case 3:
                LOGGER.info("Starting job 3");
                String[] commandMaleEmployment = {
                    "3",
                    "data/Gender_StatsData.csv",
                    "outputGenderStats/BQ3",
                    "2000",
                    "2016"
                };
                new GenderStatistics().run(commandMaleEmployment);  
                break;      
            case 2:
                LOGGER.info("Starting job 2");
                String[] commandFemaleEducationIncrease = {
                    "2",
                    "data/Gender_StatsData.csv",
                    "outputGenderStats/BQ2",
                    "2000",
                    "2016"
                };
                new GenderStatistics().run(commandFemaleEducationIncrease);        
                break;
            case 1:
                LOGGER.info("Starting job 1");
                String[] commandFemaleGraduation = {
                    "1",
                    "data/Gender_StatsData.csv",
                    "outputGenderStats/BQ1",
                    "2010",
                    "2016"
                };
                new GenderStatistics().run(commandFemaleGraduation);        
                break;
        }

    }
}