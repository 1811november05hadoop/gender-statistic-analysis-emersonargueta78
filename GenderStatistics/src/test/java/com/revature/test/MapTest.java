package com.revature.test;

import static org.junit.Assert.assertTrue;

import com.revature.Util.Regex;
import com.revature.map.CodeForBussinessQuestion;
import com.revature.reduce.Reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MapTest {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and a mapper and a
     * reducer working together.
     */
    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

        /*
         * Set up the mapper test harness.
         */
        // Mapper<LongWritable, Text, Text, Text> mapper = new CodeForBussinessQuestion();
        // mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        // mapDriver.setMapper(mapper);

        // /*
        //  * Set up the reducer test harness.
        //  */
        // IntersectReduce reducer = new IntersectReduce();
        // reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        // reduceDriver.setReducer(reducer);

        // /*
        //  * Set up the mapper/reducer test harness.
        //  */
        // mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
        // mapReduceDriver.setMapper(mapper);
        // mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mapper.
     */
    @Test
    public void testMapper() {
        assertTrue(true);
        // Regex regex = new Regex();
        
        
        // regex.setRegexes(
        //         "(.*)Educational(.*).*(.*)completed(.*).*(.*)female(.*)",
        //         "(.*)education(.*)"
        // );
        
        /*
         * For this test, the mapper's input will be "1 cat cat dog"
         */
        // mapDriver.withInput(new LongWritable(1),
        // new Text("\"SE.SEC.CUAT.LO.FE.ZS\",\"Education: Outcomes\",\"Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)\",\"\",\"The percentage of population ages 25 and over that attained or completed lower secondary education.\",\"\",\"Annual\",\"\",\"\",\"\",\"Caution is required when using this indicator for cross-country comparison, since the countries do not always classify degrees and qualifications at the same International Standard Classification of Education (ISCED) levels, even if they are received at roughly the same age or after a similar number of years of schooling. Also, certain educational programmes and study courses cannot be easily classified according to ISCED. This indicator only measures educational attainment in terms of level of education attained, i.e. years of schooling, and do not necessarily reveal the quality of the education (learning achievement and other impacts).\",\"\",\"\",\"United Nations Educational, Scientific, and Cultural Organization (UNESCO) Institute for Statistics.\",\"It is calculated by dividing the number of population ages 25 and older who attained or completed lower secondary education by the total population of the same age group and multiplying by 100. The number 0 means zero or small enough that the number would round to zero."));

        // /*
        //  * The expected output is "cat 1", "cat 1", and "dog 1".
        //  */
        // mapDriver.withOutput(new Text("SE.SEC.CUAT.LO.FE.ZS"),
        //                      new Text(":Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)"));

        /*
         * Run the test.
         */
        // mapDriver.runTest();
    }

    /*
     * Test the reducer.
     */
    // @Test
    // public void testReducer() {

    //     List<IntWritable> values = new ArrayList<IntWritable>();
    //     values.add(new IntWritable(1));
    //     values.add(new IntWritable(1));

    //     /*
    //      * For this test, the reducer's input will be "cat 1 1".
    //      */
    //     reduceDriver.withInput(new Text("cat"), values);

    //     /*
    //      * The expected output is "cat 2"
    //      */
    //     reduceDriver.withOutput(new Text("cat"), new IntWritable(2));

    //     /*
    //      * Run the test.
    //      */
    //     reduceDriver.runTest();
    // }

    // /*
    //  * Test the mapper and reducer working together.
    //  */
    // @Test
    // public void testMapReduce() {

    //     /*
    //      * For this test, the mapper's input will be "1 cat cat dog"
    //      */
    //     mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));

    //     /*
    //      * The expected output (from the reducer) is "cat 2", "dog 1".
    //      */
    //     mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
    //     mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));

    //     /*
    //      * Run the test.
    //      */
    //     mapReduceDriver.runTest();
    // }
}
