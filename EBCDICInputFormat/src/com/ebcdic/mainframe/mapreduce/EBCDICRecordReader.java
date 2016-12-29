package com.ebcdic.mainframe.mapreduce;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import net.sf.cobol2j.RecordSet;
import net.sf.cobol2j.*;
import net.sf.cobol2j.ObjectFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.xml.bind.*;

import net.sf.cobol2j.FileFormat;
import net.sf.cobol2j.RecordParseException;
import net.sf.cobol2j.RecordSet;

public class EBCDICRecordReader extends RecordReader<LongWritable, Text> {

	 private long start;
	  private long pos;
	  private long end;
	  private RecordSet rset=null;
	  private long i=0l;
	  private boolean fileProcessed = false;
	  private BufferedInputStream fileIn;
	  private BufferedInputStream inputStream;
	  private LongWritable key = null;
	  private Text value = null;
	  
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fileIn.close();
		inputStream.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (fileProcessed) {
			return 1.0f;
			} else {
			return 0.0f;
			}
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		try
		{
			String cblPath = context.getConfiguration().get(
	            copybook.COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF);

	        FileSystem fs = FileSystem.get(context.getConfiguration());

	         inputStream = new BufferedInputStream(fs.open(new Path(cblPath)));
	         
	         System.out.print(cblPath);
	         
	         JAXBContext contextjx = JAXBContext.newInstance(net.sf.cobol2j.ObjectFactory.class.getPackage().getName(),
	        		 net.sf.cobol2j.ObjectFactory.class.getClassLoader());
				Unmarshaller unmarshaller = contextjx.createUnmarshaller();
				Object o = unmarshaller.unmarshal(inputStream);
				FileFormat fF = (FileFormat) o;			
		      
				 FileSplit fileSplit = (FileSplit) split;
				 
				 start = fileSplit.getStart();
			      end = start + fileSplit.getLength();
			      final Path file = fileSplit.getPath();
			      System.out.print(fileSplit.getPath());
			   fileIn = new BufferedInputStream(fs.open(fileSplit.getPath()));
			   rset = new RecordSet(fileIn, fF);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(rset==null || !rset.hasNext())
		{
		this.fileProcessed=true;
		return false;
		}
		
		 if (key == null) {
		      key = new LongWritable();
		    }
		    if (value == null) {
		      value = new Text();
		    }

		    if (rset!=null && rset.hasNext()) {
		    	Iterator<?> fields;
				StringBuilder sb = new StringBuilder();
				try {
					i++;
					
					fields = rset.next().iterator();
					boolean afterfirst = false;
					
					while (fields.hasNext()) 
					{
						if (afterfirst) {
							sb.append('|');
						}
						String ftemp=fields.next().toString().trim().replaceAll("\\p{Cc}", "~");
						 sb.append(ftemp);//System.out.print(fields.next().toString());
						afterfirst = true;
					}
					
					pos=i;
					key.set(i);
					value.set(sb.toString());
				} 
				catch (Exception e)
				{
					e.printStackTrace();
					//System.exit(0);
				}
				
			}
		    else return false;
		return true;
	}

}
