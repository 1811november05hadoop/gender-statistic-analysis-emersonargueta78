package com.revature.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * ReadFirstRow
 */
public class ReadFirstRow {

    public Map<String,Integer> readFirstRow(String pathname) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(pathname))));
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