package com.revature.driver;
import com.revature.GenderStatistics;
import com.revature.map.Q1Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * FemaleEducation
 */
public class DriverQ2 extends Configured implements Tool {
        final private static Logger LOGGER = Logger.getLogger(DriverQ1.class);
        static{
            Q1Mapper mapper = new Q1Mapper();
            mapper.setRegexes(
                "Educational",
                "(.*) completed(.*)",
                "(.*) female(.*)");

            mapper.setYearlyRanges("2010","2016");
        }

        @Override
        public int run(String[] args) throws Exception {

                LOGGER.info("Starting Female Education");
                if (args.length != 4) {
                        System.out.printf("Usage: GenderStatistics <input1 dir> <input2 dir> <output1 dir>\n");
                        System.exit(-1);
                }

                Configuration config = new Configuration();
                Job job = new Job();
                FileSystem fs = FileSystem.get(config);

                job.setJarByClass(GenderStatistics.class);
                job.setJobName("Female Gender Statistics");

                /**
                 * Using multiple inputs with a mapper for each
                 */
                Path inputFile = new Path(args[0]);
                Path outputDir = new Path(args[1]);

                FileInputFormat.setInputPaths(job, inputFile);
                
                
                
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                
                job.setMapperClass(Q1Mapper.class);

                job.setOutputKeyClass(Text.class);
		        job.setOutputValueClass(Text.class);
                /*
                 * delete if exist
                 */
                if (fs.exists(outputDir)) {
                        fs.delete(outputDir, true);
                }

                TextOutputFormat.setOutputPath(job, outputDir);

                boolean success = job.waitForCompletion(true);
                //System.exit(success ? 0 : 1);
                return success ? 0 : 1;
        }

}