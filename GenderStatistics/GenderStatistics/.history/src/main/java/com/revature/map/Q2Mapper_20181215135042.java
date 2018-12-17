package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.util.Cleaning;

/**
 * Q2Mapper
 */
public class Q2Mapper {


    public static void main(String[] args) throws IOException {
        Cleaning cleaning = new Cleaning();
        String line = "\"Argentina\",\"ARG\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"1.7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.76038\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.77619\",\"\",\"15.26402\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        List<String> colVals = entry.getValue();
        
        StringBuilder dataVals = cleaning.getYearlyColumnVals(colVals,
        new AbstractMap.SimpleEntry<String,String>("2010", "2016"));
        System.out.println(dataVals.toString());

    }
}