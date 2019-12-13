package org.myorg;

/*
 * Sharmistha Haldar
 * shaldar@uncc.edu
 */

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FullIndex extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(FullIndex.class);
	
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new FullIndex(), args);
	    System.exit(res);
	  }

	  public int run( String[] args) throws  Exception {
			Job job  = Job.getInstance(getConf(), "fullInvertedIndex");
			job.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			return job.waitForCompletion(true)  ? 0 : 1;
		}
	  
	  public static class Map extends Mapper<Object,  Text ,  Text ,  Text > {
			private Text word  = new Text();
			private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

			public void map( Object offset,  Text lineText,  Context context)
					throws  IOException,  InterruptedException {

				String line  = lineText.toString();
				Text currentWord  = new Text();
				Text currentFile = new Text();
				
				// retrieving the file Name and storing in fileName
				String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				for ( String word  : WORD_BOUNDARY .split(line)) {
					if (word.isEmpty()) {
						continue;
					}
					
					String temp = fileName +'@'+ offset;
	                
					currentWord  = new Text(word);
					currentFile = new Text(temp);
					
					// Mapping word with filename 
					context.write(currentWord, currentFile);
				}
				
			}
		}
	  
	  
		public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
			
			private Text result = new Text();
			
			public void reduce( Text word,  Iterable<Text> docs,  Context context)
					throws IOException,  InterruptedException {
				
				StringBuilder sb = new StringBuilder();
				boolean first = true;
				
				// logic to sort the documents
				ArrayList<String> listDoc = new ArrayList<String>();
			      while (docs.iterator().hasNext()) {
			    	  
			          listDoc.add(docs.iterator().next().toString().trim());
			      }
			      
			    LinkedHashSet<String> hashset = new LinkedHashSet<>(listDoc);
			    ArrayList<String> sortDoc = new ArrayList<>(hashset);
			    
			    Collections.sort(sortDoc);
				
				for(String doc: sortDoc)
				{ 
					if(first){
						first = false;
					}
					else{
						sb.append("");
					}
					
					if(sb.lastIndexOf(doc.toString())<0)
					{
						sb.append(doc.toString() + "+");
							
					}					
					
				}
				
				
				result.set(sb.toString().substring(0, sb.length() - 1));				
				word.set(word.toString() + " : ");
				
				context.write(word,result);
				
			}
		}
 	 	  

}
