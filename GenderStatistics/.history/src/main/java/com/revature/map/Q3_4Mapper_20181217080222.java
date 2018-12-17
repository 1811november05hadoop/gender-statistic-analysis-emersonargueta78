package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.util.Cleaning;
import com.revature.util.GenderStatsCols;
import com.revature.util.Globals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q3_4Mapper extends Mapper<LongWritable, Text, Text, Text> implements Globals {
    private static String[] yearlyRanges;
    private static String[] patternToMatch;
    private Cleaning cleaning = new Cleaning();

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException { 
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        List<String> colVals = entry.getValue();
       
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);
        
        if(
            
            indicatorName.contains(patternToMatch[0])
            && indicatorName.contains(patternToMatch[1])
            && indicatorName.matches(patternToMatch[2])
        ){
            StringBuilder dataVals = cleaning
                .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
            
            
            String bq1DataVals = percentChange(dataVals.toString());
            //String bq1DataVals = "";

            context.write(new Text(indicatorName+"|"+country)
                ,new Text(bq1DataVals));   
        }   
    }

    private String percentChange(String dataVals) {
        StringBuilder retStr = new StringBuilder();
        List<String> dataValsList = cleaning.splitDelimiter(dataVals,",");
        
        for (int index = 0; (index+1) < dataValsList.size(); index++) {
            String [] yearAndData = dataValsList.get(index).split(":",-1);
            String year = yearAndData[0];
            String data = yearAndData[1];
            String [] nextYearAndData = dataValsList.get(index+1).split(":",-1);
            String nextYear = nextYearAndData[0];
            String nextData = nextYearAndData[1];

            if(!data.isEmpty() && !nextData.isEmpty()){
                retStr.append(year+"-"+nextYear+":")
                      .append(percentChangeHelper(data,nextData))
                      .append(",");
            }    
        }
        return retStr.toString();
    }

    private String percentChangeHelper(String data, String nextData) {
        double dataD = Double.valueOf(data);
        double dataDNext = Double.valueOf(nextData);

        return String.valueOf(dataDNext - dataD);
    }

    @Override
    public void setRegexes(String... patternToMatch) { 
        this.patternToMatch = patternToMatch;
    }


    @Override
    public String[] getRegexes() {
        return this.patternToMatch;
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