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
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        
        mapper.setRegexes(
            "Educational(.*)",
            "(.*)Educational (.*).*(.*) completed(.*).*(.*) female(.*)",
            "United States");

        mapper.setYearlyRanges("2010","2016");
        mapDriver = new MapDriver<LongWritable,Text,MapWritable,MapWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q2TestMap() throws IOException {
        
        String testStr = "\"United States\",\"USA\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5\",\"\",\"4\",\"\",\"\",\"5\",\"3\",\"2\","; 
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
            map1ValStr.toString().equals("United States"),
            map2keyStr.toString().equals("-1.5"),
            map2Val.equals(new IntWritable(1))
        };
        assertArrayEquals(exepecteds, actuals);  
    }    
}