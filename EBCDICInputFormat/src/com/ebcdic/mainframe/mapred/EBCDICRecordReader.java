package com.ebcdic.mainframe.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.*;

import net.sf.cobol2j.FileFormat;
import net.sf.cobol2j.RecordParseException;
import net.sf.cobol2j.RecordSet;

public class EBCDICRecordReader implements RecordReader<LongWritable, Text> 
{

	  private long start;
	  private long pos;
	  private long end;
	  private RecordSet rset;
	  private long i=0l;
	  private boolean fileProcessed = false;
	  private BufferedInputStream fileIn;
	  private BufferedInputStream inputStream;
	  
	  private static String fieldDelimiter = new Character((char) 0x01).toString();

	  public EBCDICRecordReader(FileSplit genericSplit, JobConf job)
	      throws IOException {
		  rset=null;
	    try {
	      String cblPath = job.get(copybook.COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF);

	      if (cblPath == null) 
	      {
	        if (job != null) 
	        {
	          MapWork mrwork = Utilities.getMapWork(job);

	          if (mrwork == null)
	          {
	            System.out.println("Missing Paths");
	          }
	          Map<String, PartitionDesc> map = mrwork.getPathToPartitionInfo();
	          
	          for (Map.Entry<String, PartitionDesc> pathsAndParts : map.entrySet())
	          {
	            Properties props = pathsAndParts.getValue().getProperties();
	            cblPath = props
	                .getProperty(copybook.COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF);
	            break;
	          }
	        }
	      }
	      
	      FileSystem fs = FileSystem.get(job);
	      inputStream = new BufferedInputStream(
	          fs.open(new Path(cblPath)));
	      JAXBContext contextjx = JAXBContext contextjx = JAXBContext.newInstance(net.sf.cobol2j.ObjectFactory.class.getPackage().getName(),
	        		 net.sf.cobol2j.ObjectFactory.class.getClassLoader());
			Unmarshaller unmarshaller = contextjx.createUnmarshaller();
			Object o = unmarshaller.unmarshal(inputStream);
			FileFormat fF = (FileFormat) o;			
	      
			
			 FileSplit split = (FileSplit) genericSplit;

		      start = split.getStart();
		      end = start + split.getLength();
		    //  final Path file = split.getPath();

		      fileIn = new BufferedInputStream(fs.open(split
		          .getPath()));
		       rset = new RecordSet(fileIn, fF);
	    }
	    catch (Exception e) 
	    {
	        e.printStackTrace();
	      } 
	  }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fileIn.close();
		inputStream.close();
	}

	@Override
	public LongWritable createKey() {
		// TODO Auto-generated method stub
		 return new LongWritable();
	}

	@Override
	public Text createValue() {
		// TODO Auto-generated method stub
		 return new Text();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return pos;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		if (fileProcessed) {
			return 1.0f;
			} else {
			return 0.0f;
			}
		
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		Iterator<?> fields;
		StringBuilder sb = new StringBuilder();
		if (rset!=null && rset.hasNext()) {
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
				return true;
			} 
			catch (RecordParseException ex) {
				//displayErrorRecord(ex, Collections.<FieldFormat>emptyList());
				ex.printStackTrace();
				//System.exit(0);
			}
			catch (Exception e)
			{
				e.printStackTrace();
				//System.exit(0);
			}
			
		}
		this.fileProcessed=true;
		return false;
	}
	      
}
class copybook {
	public static final String COPYBOOK_INPUTFORMAT_HDFS_PATH_CONF = "copybook.inputformat.hdfs.path";
}
