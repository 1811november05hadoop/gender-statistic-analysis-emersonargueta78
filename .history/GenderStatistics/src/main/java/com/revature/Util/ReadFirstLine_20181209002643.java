package com.revature.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * ReadFirstLine
 */
public final class ReadFirstLine {

    public static String readLine(String fileNamePath){
        String line = null;
        
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileNamePath));
            line = reader.readLine();
            reader.close();       
        } catch (IOException e) {
            //TODO: handle exception
        }
        
        return line;
    }  
    
}