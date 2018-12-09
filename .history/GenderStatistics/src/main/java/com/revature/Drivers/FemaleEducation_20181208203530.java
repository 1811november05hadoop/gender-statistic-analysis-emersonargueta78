package com.revature.Drivers;

import com.revature.GenderStatistics;
import com.revature.map.ColumnValueMapper;
import com.revature.reduce.ReduceClean;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * FemaleEducation
 */
public class FemaleEducation extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.out.printf(
                    "Usage: WordCount <input dir> <output dir>\n");
            System.exit(-1);
        }

        Job jobExtractCodes = new Job();
        Job job = new Job();

        job.setJarByClass(GenderStatistics.class);

        job.setJobName("Female Gender Statistics");

        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(jobExtractCodes, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(jobExtractCodes, new Path(args[2]));
        
        
        

        job.setMapperClass(ColumnValueMapper.class);
        // job.setPartitionerClass(null);
        // job.setNumReduceTasks(1);
        // // job.setCombinerClass(null);
        // job.setReducerClass(ReduceClean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
        return 0;
	}
    
}