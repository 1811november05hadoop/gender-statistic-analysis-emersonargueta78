package com.revature;

import com.revature.map.Q1Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Q1Test
 */
public class Q1Test{
    MapDriver<IntWritable,Text,MapWritable,IntWritable> mapDriver;
    //ReduceDriver<> reducer;

    @Before
    public void setUp() {
        Q1Mapper mapper = new Q1Mapper();
        mapDriver = new MapDriver<IntWritable,Text,MapWritable,IntWritable>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void q1TestMap() {
        
    }

    
}