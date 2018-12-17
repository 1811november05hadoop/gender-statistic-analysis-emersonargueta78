package com.revature;
import java.io.IOException;


import com.revature.map.Q1Mapper;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.junit.Before;
import org.junit.Test;

/**
 * Q1Test
 */
public class Q5Test{
    MapDriver<LongWritable,Text,Text,Text> mapDriver;
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        
        mapper.setRegexes(
            "Educational",
            "completed",
            "(.*) male(.*)");

        mapper.setYearlyRanges("2010","2016");
        mapDriver = new MapDriver<LongWritable,Text,Text,Text>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q1TestMap() throws IOException {
        
        String testStr = "\"Argentina\",\"ARG\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, male (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"1.7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.76038\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.77619\",\"\",\"15.26402\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","; 
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        mapDriver.withOutput(new Text("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)|Argentina"),
         new Text("2010:,2011:,2012:,2013:,2014:,2015:,2016:,"));
        mapDriver.runTest();
    }    
}