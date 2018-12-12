package com.revature.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ReadFirstRow
 */
public class ReadFirstRow {

    public Map<String,Integer> readFirstRow(String pathname) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(pathname));
        String firstRow = reader.readLine();
        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(firstRow);
        ArrayList<String> arry = entry.getValue();

        Map<String,Integer> map = new HashMap<>();
        int indx = 0;
        
        for (String colVal : arry) {
            map.put(colVal,indx);
            indx++;
        }

        reader.close();
        return map;
    }
    

}