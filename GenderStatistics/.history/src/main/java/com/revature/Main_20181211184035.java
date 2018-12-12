package com.revature;

/**
 * Main
 */
public class Main {

    public static void main(String[] args) throws Exception {
        switch(Integer.valueOf(args[0])){
            case 5:
                System.out.println("Starting job 5");
                // String[] commandFemaleGraduation = {};
                // new GenderStatistics().run(commandFemaleGraduation);        
            case 4:
                System.out.println("Starting job 4");
                // String[] commandFemaleEducation = {};
                // new GenderStatistics().run(commandFemaleEducation);        
            case 3:
                System.out.println("Starting job 3");
                // String[] commandMaleEmployment = {};
                // new GenderStatistics().run(commandMaleEmployment);        
            case 2:
                System.out.println("Starting job 2");
                // String[] commandFemaleEmployment = {};
                // new GenderStatistics().run(commandFemaleEmployment);        
            case 1:
                System.out.println("Starting job 1");
                String[] commandEmploymentEducationRelationship = {};
                new GenderStatistics().run(commandEmploymentEducationRelationship);        
                break;
            
        }

    }
}