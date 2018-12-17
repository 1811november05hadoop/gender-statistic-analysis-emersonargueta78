package com.revature.util;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Cleaning
 */
public class Cleaning {
    public static void main(String[] args) {
        String line = "2010:,2011:,2012:,2013:,2014:,2015:,2016:,";
        Cleaning cleanin = new Cleaning();
        List<String> newLine = cleanin.splitDelimiter(line,",");
        for (String dataVal : newLine) {
            System.out.println(dataVal);
        }

    }
    
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
            retStrBld.append(yearStr+":"+colVals.get(dataCols.get(yearStr)) +",");

        }

        return retStrBld;
        
    }
    public List<String> splitDelimiter(String line,String delim) {
        List<String> retList = Arrays.asList(line.split(delim));
        return retList;
    }
}