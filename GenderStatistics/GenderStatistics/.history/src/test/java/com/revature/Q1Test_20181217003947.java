package com.revature;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.List;

import com.revature.map.Q1Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 * Q1Test
 */
public class Q1Test{
    MapDriver<LongWritable,Text,Text,Text> mapDriver;
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        
        mapper.setRegexes(
            "Educational(.*)",
            "(.*)Educational (.*).*(.*) completed(.*).*(.*) female(.*)");

        mapper.setYearlyRanges("2010","2016");
        mapDriver = new MapDriver<LongWritable,Text,Text,Text>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q1TestMap() throws IOException {
        
        String testStr = "\"Argentina\",\"ARG\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"1.7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.76038\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.77619\",\"\",\"15.26402\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","; 
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        
        List<Pair<Text,Text>> expectedOutput = mapDriver.run();
        
        StringBuilder map1keyStr = new StringBuilder();
        StringBuilder map1ValStr = new StringBuilder();

        Text text1Expect = expectedOutput.get(0).getFirst();
        Text text2Expect = expectedOutput.get(0).getSecond();
         
        boolean[] exepecteds = {true,true,true};
        boolean[] actuals = {
            map1keyStr.toString().equals("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)"),
            text1Expect.toString().equals("Argentina"),
            text2Expect.toString().equals("2010:,2011:,2012:,2013:,2014:,2015:,2016:,")   
        };
        assertArrayEquals(exepecteds, actuals);  
    }    
}