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
public class Q2Test{
    MapDriver<LongWritable,Text,MapWritable,MapWritable> mapDriver;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        
        mapper.setRegexes(
            "Educational(.*)",
            "(.*)Educational (.*).*(.*) completed(.*).*(.*) female(.*)");

        mapper.setYearlyRanges("2000","2016");
        mapDriver = new MapDriver<LongWritable,Text,MapWritable,MapWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q1TestMap() throws IOException {
        
        String testStr = "\"Cambodia\",\"KHM\",\"Educational attainment, at least completed lower secondary, population 25+, male (%) (cumulative)\",\"SE.SEC.CUAT.LO.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.39346\",\"23.86751\",\"22.2097\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","; 
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        
        List<Pair<MapWritable,MapWritable>> expectedOutput = mapDriver.run();
        
        StringBuilder map1keyStr = new StringBuilder();
        StringBuilder map2keyStr = new StringBuilder();
        StringBuilder map1ValStr = new StringBuilder();
        IntWritable map2Val = null;

        MapWritable map1Expect = expectedOutput.get(0).getFirst();
        MapWritable map2Expect = expectedOutput.get(0).getSecond();

        for (Writable key : map1Expect.keySet()) {
          map1keyStr.append(key.toString());
          map1ValStr.append(map1Expect.get(key));

        }
        
        for (Writable key : map2Expect.keySet()) {
            map2keyStr.append(key.toString());
            map2Val = (IntWritable) map2Expect.get(key);
        }
         
        boolean[] exepecteds = {true,true,true,true};
        boolean[] actuals = {
            map1keyStr.toString().equals("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)"),
            map1ValStr.toString().equals("Argentina"),
            map2keyStr.toString().equals("2010:,2011:,2012:,2013:,2014:,2015:,2016:,"),
            map2Val.equals(new IntWritable(1))
        };
        assertArrayEquals(exepecteds, actuals);  
    }    
}