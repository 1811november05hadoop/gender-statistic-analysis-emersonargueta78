package com.revature;

import com.revature.map.Q1Mapper;
import com.revature.util.Globals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
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
        mapDriver = new MapDriver<LongWritable,Text,MapWritable,MapWritable>();
        mapDriver.setMapper(mapper);
        Q1Mapper.yearLowerRange = "2010";
        Q1Mapper.yearLowerRange = "2016";
    }

    @Test
    public void q1TestMap() {
        // Globals globals = new Globals();
        // globals.setYearlyRanges("1960","2016");
        String testStr = "\"Arab World\",\"ARB\",\"Access to anti-retroviral drugs, female (%)\",\"SH.HIV.ARTC.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.0154427732646988\",\"0.0687530803351405\",\"0.103438795848702\",\"0.704228689601689\",\"1.01911528204711\",\"1.6988591086665\",\"2.72210741878432\",\"3.58585441218741\",\"5.61150904455506\",\"7.3503927118048\",\"8.64536596419669\",\"10.0595271778428\",\"12.3582578856259\",\"15.2250279997013\",\"17.7513862812732\",\"21.102335739509\",\"\",";
        String testData = "\"8.64536596419669\",\"10.0595271778428\",\"12.3582578856259\",\"15.2250279997013\",\"17.7513862812732\",\"21.102335739509\",\"\",";
        
        //mapDriver.withInput(new IntWritable(1), new Text(testStr));
        MapWritable mapKey = new MapWritable();
        MapWritable mapVal = new MapWritable();
        mapKey.put(new Text("SH.HIV.ARTC.FE.ZS"), new Text("Arab World"));
        mapVal.put(new Text(testData), new IntWritable(1));
        mapDriver.withOutput(mapKey, mapVal);
        
    }
    
}