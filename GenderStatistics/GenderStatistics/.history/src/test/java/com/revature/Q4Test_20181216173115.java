package com.revature;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.List;

import com.revature.map.Q4Mapper;

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
public class Q4Test{
    MapDriver<LongWritable,Text,MapWritable,MapWritable> mapDriver;

    @Before
    public void setUp() {
        Q4Mapper mapper = new Q4Mapper();
        
        mapper.setRegexes(
            "Employment(.*)",
            "(.*)Employment (.*).*(.*) population(.*).*(.*) male(.*)",
            "United States");

        mapper.setYearlyRanges("2000","2016");
        mapDriver = new MapDriver<LongWritable,Text,MapWritable,MapWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q3TestMap() throws IOException {
        
        String testStr = "\"United States\",\"USA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.39346\",\"23.86751\",\"22.2097\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","; 
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
            map1keyStr.toString().equals("Employment to population ratio, 15+, male (%) (modeled ILO estimate)"),
            map1ValStr.toString().equals("United States"),
            map2keyStr.toString().equals("2007-2008:1.4740499999999983,2008-2009:-1.6578099999999978,"),
            map2Val.equals(new IntWritable(1))
        };
        assertArrayEquals(exepecteds, actuals);  
    }    
}