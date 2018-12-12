package com.revature.Drivers;

import com.revature.GenderStatistics;
import com.revature.map.CodeForFemaleEducation;
import com.revature.map.ColumnValueMapper;
import com.revature.reduce.IntersectReduce;

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

/**
 * FemaleEducation
 */
public class FemaleEducation extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {

                return 0;
        }

}