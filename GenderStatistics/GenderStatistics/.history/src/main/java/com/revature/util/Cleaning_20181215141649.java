package com.revature.util;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    public StringBuilder getYearlyColumnVals(List<String> colVals,Entry<String,String> range) throws IOException {
        StringBuilder retStrBld = new StringBuilder();
        
        Map<String,Integer> dataCols = new GenderStatsCols().getDataCols();
        
        String lowerRangeStr = range.getKey();
        String upperRangeStr = range.getValue();
        
        int lowerRange = Integer.valueOf(lowerRangeStr);
        int upperRange = Integer.valueOf(upperRangeStr);

        for(int year = lowerRange; year<=upperRange;year++){
            String yearStr = String.valueOf(year);
            retStrBld.append(yearStr + ":"+colVals.get(dataCols.get(yearStr)) +",");    
        }

        return retStrBld;
        
    }
}