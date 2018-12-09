package com.revature.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ReadFirstLine
 */
public class ReadFirstLine {
    public static String readLine(String fileNamePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileNamePath));
        List<String> lines = new ArrayList<>();
        String line = null;
        line = reader.readLine();
        reader.close();   
        return line;
    }
    
    
    
}