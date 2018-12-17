package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

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
public class Q1Mapper extends Mapper<LongWritable,Text,MapWritable,MapWritable> implements Globals{
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

        
        StringBuilder dataVals = cleaning
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);

        
        
        if(
            indicatorName.matches(regexes[0])
            && indicatorName.matches(regexes[1])
        ){
            MapWritable mapKey = new MapWritable();          
            MapWritable mapVal = new MapWritable();
            String bq1DataVals = dataValsLess30(dataVals.toString());
            //String bq1DataVals = dataVals.toString();

            mapKey.put(new Text(indicatorName), new Text(country));
            mapVal.put(new Text(bq1DataVals),new IntWritable(1));
            context.write(mapKey,mapVal);
            
            
        }
        
    }
    public static void main(String[] args) throws IOException {
        Cleaning cleaning = new Cleaning();
        String line = "\"Argentina\",\"ARG\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"1.7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.76038\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.77619\",\"\",\"15.26402\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"20.0\",";
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        List<String> colVals = entry.getValue();
        
        StringBuilder dataVals = cleaning.getYearlyColumnVals(colVals,
        new AbstractMap.SimpleEntry<String,String>("2010", "2016"));
        System.out.println( new Q1Mapper().dataValsLess30(dataVals.toString()));
        
    }

    private String dataValsLess30(String dataVals) {
        StringBuilder retStr = new StringBuilder();
        retStr.append("");
        List<String> dataValsList = cleaning.splitDelimiter(dataVals,",");
        System.out.println(dataVals);
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