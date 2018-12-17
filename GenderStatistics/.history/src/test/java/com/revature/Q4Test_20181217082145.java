package com.revature;

import java.io.IOException;


import com.revature.map.Q3_4Mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Q1Test
 */
public class Q4Test{
    MapDriver<LongWritable,Text,Text,Text> mapDriver;
    
    @Before
    public void setUp() {
        Q3_4Mapper mapper = new Q3_4Mapper();
        mapper.setRegexes(
            "Employment",
            "population",
            "(.*) female(.*)"
            );
        mapper.setYearlyRanges("2000","2016");
        mapDriver = new MapDriver<LongWritable,Text,Text,Text>();
        mapDriver.setMapper(mapper);
        
    }

    @Test
    public void q4TestMap() throws IOException {
        
        String testStr = "\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.39346\",\"23.86751\",\"22.2097\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","; 
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        mapDriver.withOutput(new Text("Employment to population ratio, 15+, female (%) (modeled ILO estimate)|United States"),
            new Text("2007-2008:1.4740499999999983,2008-2009:-1.6578099999999978,"));
        mapDriver.runTest();
    }
        
}