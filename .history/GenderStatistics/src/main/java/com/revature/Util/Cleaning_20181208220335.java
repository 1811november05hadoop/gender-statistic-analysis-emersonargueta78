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
        //Retrieving all column values
        for (String phrase : line.split("\",\"")) {
            stringRow.append(phrase.replace("\"", "").replace("\",",""))
                     .append(" ");       
            colValsList.add(phrase.replace("\"", "").replace("\",",""));       
        }
        return new AbstractMap.SimpleEntry<StringBuilder,ArrayList<String>>(stringRow,colValsList);
    }
}