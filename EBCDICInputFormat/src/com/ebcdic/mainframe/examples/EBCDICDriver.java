package com.ebcdic.mainframe.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.ebcdic.mainframe.mapreduce.*;

public class EBCDICDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Mapreduce App");

		// Set driver class
		job.setJarByClass(EBCDICDriver.class);

		// Set Input & Output Format
		job.setInputFormatClass(EBCDICInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set Mapper & Reducer Class
		job.setMapperClass(CMapper.class);

		// No. of reduce tasks, equals no. output file
		job.setNumReduceTasks(0);
		
		// HDFS input and output path
	//	EBCDICInputFormat.setCopybookHdfsPath(conf, args[2]);
		EBCDICInputFormat.setCopybookHdfsPath(job.getConfiguration(), args[2]);
		EBCDICInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
