package com.revature.Util;

/**
 * ReadFirstLine
 */
public class ReadFirstLine {
    public static void readLine() {
        BufferedReader reader = new BufferedReader(new FileReader(<<your file>>));
        List<String> lines = new ArrayList<>();
        String line = null;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }    
    }
    
    
    
}