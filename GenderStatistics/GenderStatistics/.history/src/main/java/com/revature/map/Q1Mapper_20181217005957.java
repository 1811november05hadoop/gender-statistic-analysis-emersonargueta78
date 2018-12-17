package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.util.Cleaning;
import com.revature.util.GenderStatsCols;
import com.revature.util.Globals;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q1Mapper extends Mapper<LongWritable,Text,Text,Text> implements Globals{
    private static String[] yearlyRanges;
    private static String[] regexes;
    private Cleaning cleaning = new Cleaning();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        StringBuilder stringRow = entry.getKey();
        List<String> colVals = entry.getValue();

        
        
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);

        
        
        if(
            indicatorName.contains(regexes[0])
            && indicatorName.matches(regexes[1])
        ){
            String bq1DataVals = "";
            StringBuilder dataVals = cleaning
                .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
            //String bq1DataVals = dataValsLess30(dataVals.toString());
            context.write(new Text(indicatorName+"|"+country),new Text(bq1DataVals));
        }
        
    }

    private String dataValsLess30(String dataVals,String indicatorName) {
        StringBuilder retStr = new StringBuilder();
        retStr.append("");
        List<String> dataValsList = cleaning.splitDelimiter(dataVals,",");
        for (String dataVal : dataValsList) {
            
            String [] yearAndData = dataVal.split(":",-1);
            
            String year = yearAndData[0];
            String data = yearAndData[1];
            
            if(data.isEmpty()){
                retStr.append(year+":,");
                continue;
            }
            else{
                double dataD = Double.valueOf(data);
                if(dataD > 30.0){
                    retStr.append(year+":"+data+",");
                }
            }
        }        
        return retStr.toString();
    }

    @Override
    public void setRegexes(String... regexes) { 
        this.regexes = regexes;
    }


    @Override
    public String[] getRegexes() {
        return this.regexes;
    }

    @Override
    public void setYearlyRanges(String... ranges) {
        this.yearlyRanges = ranges;
    }

    @Override
    public String[] getYearlyRanges() {
        return this.yearlyRanges;
    }
    
}