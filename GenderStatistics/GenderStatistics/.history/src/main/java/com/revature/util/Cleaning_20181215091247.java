package com.revature.Util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map.Entry;

/**
 * Cleaning
 */
public class Cleaning {
    
    public Entry<StringBuilder,ArrayList<String>> splitAndClean(String line){
        ArrayList<String> colValsList = new ArrayList<>();    
        StringBuilder stringRow = new StringBuilder();
        String removeCommasAtEnd = ",+$";
        String removeQuotesAtBeggining = "^\"+";
        //Retrieving all column values
        String lineToClean = line.trim()  
                                 .replaceAll(removeCommasAtEnd, "")                    
                                 .replaceAll(removeQuotesAtBeggining, "");
            

        for (String phrase : lineToClean.split("\",\"")) {
            stringRow.append(phrase.replace("\"", ""))
                     .append(":");
            colValsList.add(phrase.replace("\"", ""));       
        }

        return new AbstractMap.SimpleEntry<StringBuilder,ArrayList<String>>(stringRow,colValsList);
    }
}