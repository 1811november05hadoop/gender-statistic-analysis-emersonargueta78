package com.revature;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.List;

import com.revature.map.Q2Mapper;

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
public class Q2Test{
    MapDriver<LongWritable,Text,Text,Text> mapDriver;
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q2Mapper mapper = new Q2Mapper();
        
        mapper.setRegexes(
            "Educational",
            "completed",
            "female",
            "United States");

        mapper.setYearlyRanges("2000","2016");
        mapDriver = new MapDriver<LongWritable,Text,Text,Text>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q2TestMap() throws IOException {
        
        String testStr = "\"United States\",\"USA\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5\",\"3\",\"2\",";
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        mapDriver.withOutput(new Text("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)|United States"),
            new Text("2014-2015:-1.0,2015-2016:-0.5,"));
        mapDriver.runTest();
    }    
}