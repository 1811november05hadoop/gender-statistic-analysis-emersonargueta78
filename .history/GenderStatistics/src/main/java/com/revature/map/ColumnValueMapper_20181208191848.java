package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ColumnValueMapper
 */
public class ColumnValueMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    //Used to extract rows with words education and female
    final private static String regex = "education.*";
    final private static String regex1 = "(.*)education(.*).*(.*)female(.*)";

    public static void main(String[] args) {
        String str = "\"education\"\"who is that there\"\"female\"";
        String str1 = "education who is there female";
        System.out.println("testing");
        if(str.matches(regex1)){
            System.out.println("hello passed str");
        }

        if(str1.matches(regex)){
            System.out.println("hello passed str1");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        List<String> colValsList = new ArrayList<>();    
        
        //Retrieving all column values
        for (String phrase : line.split("\",\"")) {
            
            //if(!phrase.matches("") && !phrase.matches("\",")){
                colValsList.add(phrase.replace("\"", "").replace("\",",""));       
            //}        
        }
        //Remove unceccessary data
        getCountryAndYearData(colValsList,context,6);
        
    }

    private void getCountryAndYearData(List<String> colValsList, Context context,int rangeOfYears)
     throws IOException, InterruptedException {
        
        //Getting the country
        StringBuilder graduateData = new StringBuilder()
                                    .append(colValsList.get(GenderStatsCols.COUNTRY))
                                    .append(" ");
        
        //Getting the last 6 years of values 
        for (int i = GenderStatsCols.YEAR.length-1,j=0;j<rangeOfYears; i--,j++) {
            graduateData.append(colValsList.get(GenderStatsCols.YEAR[i]))
                        .append(" ");
            
        }
        
        //Getting only necessary data
        if(graduateData.toString().matches(regex)){
            System.out.println("the graduate data: "+graduateData.toString());
            //Writing the valuable col vals to context
            context.write(new Text(graduateData.toString()), new IntWritable(1));
        }
        
    }
    
}