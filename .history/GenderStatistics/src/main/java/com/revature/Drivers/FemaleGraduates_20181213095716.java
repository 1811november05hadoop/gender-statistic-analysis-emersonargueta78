package com.revature.Drivers;

import com.revature.GenderStatistics;
import com.revature.Util.Regex;
import com.revature.map.CodeForBussinessQuestion;
import com.revature.map.FemaleGraduatesLessThan30;
import com.revature.reduce.Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * FemaleEducation
 */
public class FemaleGraduates extends Configured implements Tool {

    public static int lowerYearRange;
    public static int upperYearRange;
    final private static Logger LOGGER =  Logger.getLogger(FemaleGraduates.class);
    
    final static private Regex regex = new Regex();
        
    static{
            regex.setRegexes(
                                "(.*)Educational(.*).*(.*)completed(.*).*(.*)female(.*)",
                                "(.*)education(.*)"
                            );
    }

    @Override
    public int run(String[] args) throws Exception {
        LOGGER.info("Starting Female Graduates");
        if (args.length != 5) {
            System.out.printf("Usage: GenderStatistics <input1 dir> <input2 dir> <output1 dir>\n");
            System.exit(-1);
        }
        
        lowerYearRange = Integer.valueOf(args[3]);
        upperYearRange = Integer.valueOf(args[4]);

        Configuration config = new Configuration();
        Job job = new Job();
        FileSystem fs = FileSystem.get(config);

        job.setJarByClass(GenderStatistics.class);
        job.setJobName("Female Gender Statistics");

        /**
         * Using multiple inputs with a mapper for each
         */
        Path inputFile1 = new Path(args[0]);
        Path inputFile2 = new Path(args[1]);
        Path outputDir = new Path(args[2]);

        MultipleInputs.addInputPath(job, inputFile2, TextInputFormat.class, FemaleGraduatesLessThan30.class);

        MultipleInputs.addInputPath(job, inputFile1, TextInputFormat.class, CodeForBussinessQuestion.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(Reduce.class);

        job.setOutputFormatClass(TextOutputFormat.class);

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