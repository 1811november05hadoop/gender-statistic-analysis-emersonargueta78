package com.revature.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * ReadFirstRow
 */
public class ReadFirstRow {

    public Map<Integer,String> readFirstRow() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("GenderStatistics/data/Gender_StatsSeries.csv"));
        String firstRow = reader.readLine().replace("\"", "");
        String []arry = firstRow.split(",");

        Map<Integer,String> map = new HashMap<>();
        int indx = 0;
        
        for (String colVal : arry) {
            map.put(indx, colVal);
            indx++;
        }

        reader.close();
        return map;
    }
    

}