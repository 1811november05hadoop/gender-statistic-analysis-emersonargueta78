package com.revature;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.revature.map.Q1Mapper;
import com.revature.util.Globals;

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
    MapDriver<LongWritable,Text,MapWritable,MapWritable> mapDriver;
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        
        mapper.setRegexes(
            "Educational(.*)",
            "(.*)Educational (.*).*(.*) completed(.*).*(.*) female(.*)");

        mapper.setYearlyRanges("2010","2016");
        mapDriver = new MapDriver<LongWritable,Text,MapWritable,MapWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q1TestMap() throws IOException {
        // Globals globals = new Globals();
        // globals.setYearlyRanges("1960","2016");
        String testStr = "\"Argentina\",\"ARG\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"1.7\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2.5\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"11.76038\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"12.77619\",\"\",\"15.26402\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
        //String testData = "\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
        String testData = ",,,,,,,";
        mapDriver.withInput(new LongWritable(1), new Text(testStr));
        MapWritable mapKey = new MapWritable();
        MapWritable mapVal = new MapWritable();
        
        mapKey.put(new Text("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)"),
         new Text("Argentina"));
        
        mapVal.put(new Text(testData), new IntWritable(1));
        mapDriver.withOutput(mapKey, mapVal);
        mapDriver.run();
        
        List<Pair<MapWritable,MapWritable>> expectedOutput = mapDriver.getExpectedOutputs();
        StringBuilder map1keyStr = new StringBuilder();
        StringBuilder map2keyStr = new StringBuilder();
        StringBuilder map1ValStr = new StringBuilder();
        StringBuilder map2ValStr = new StringBuilder();

        for (Writable key : expectedOutput.get(0).getFirst().keySet()) {
          keyStr.append(key.toString());
        }
        assertTrue(keyStr.toString().equals("Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)"));  

        

        //mapDriver.runTest();

        
    }
    
}