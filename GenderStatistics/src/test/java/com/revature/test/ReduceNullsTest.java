package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import com.revature.map.ColumnValueMapper;
import com.revature.reduce.ReduceClean;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class ReduceNullsTest {
	private static final Logger LOGGER = Logger.getLogger(ReduceNullsTest.class);

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	/*
	 * arguments: [0, 1](K,V): input of mapper [2, 3](K,V): output of mapper [4,
	 * 5](K,V): output of reducer
	 */
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		ColumnValueMapper columnValueMapper = new ColumnValueMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(columnValueMapper);
		
		ReduceClean reduceClean = new ReduceClean();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reduceClean);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(columnValueMapper);
		mapReduceDriver.setReducer(reduceClean);
			
	}
	
	@Test
	public void testMapper() {
		/*
		 * Mock input
		 * simulating a line of the file, not a file
		 */
		mapDriver.withInput(new LongWritable(1),new Text("\"\",\"\",\"\""));
		LOGGER.trace("\"\",\"\",\"\"");
		//expected output
		
		mapDriver.withOutput(new Text("\""), new IntWritable(1));
		mapDriver.withOutput(new Text(""), new IntWritable(1));
		mapDriver.withOutput(new Text("\""), new IntWritable(1));
		
		
		mapDriver.runTest();
	}
	
	@Test
	public void testSumReducer() {
		List<IntWritable> values = new ArrayList<>();
		values.add(new IntWritable(1));
		
		
		reduceDriver.withInput(new Text("\""), values);
		reduceDriver.withInput(new Text(""), values);
		reduceDriver.withInput(new Text("\""), values);
		reduceDriver.withInput(new Text("cat"), values);
		reduceDriver.withOutput(new Text("cat"), new IntWritable(1));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\",\"cat\",\"\","));
		
		
		mapReduceDriver.withOutput(new Text("cat"), new IntWritable(1));
		
		//builder concept; slightly more efficient
		/*
		mapReduceDriver
		.withOutput(new Text("cat"), new IntWritable(2))
		.withOutput(new Text("dog"), new IntWritable(1));
		*/
		
		mapReduceDriver.runTest();
	}

}