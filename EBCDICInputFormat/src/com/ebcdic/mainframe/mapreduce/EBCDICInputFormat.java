package com.ebcdic.mainframe.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class EBCDICInputFormat extends FileInputFormat<LongWritable, Text>{

	  @Override
	  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    return new EBCDICRecordReader();
	  }

	  public static void setCopybookHdfsPath(Configuration config, String value) {
	    config.set(copybook.COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF, value);
	  }
}

class copybook {
	public static final String COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF = "copybook.inputformat.hdfs.path";
}

