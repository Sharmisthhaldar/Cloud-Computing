package org.myorg;

/*
 * Sharmistha Haldar
 * shaldar@uncc.edu
 */

import java.io.*;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class QueryIndex extends Configured implements Tool{

	private static final Logger LOG = Logger.getLogger(QueryIndex.class);
	
	  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new QueryIndex(), args);
		    System.exit(res);
		  }
	  
	  public int run( String[] args) throws  Exception {
			Job job  = Job.getInstance(getConf(), "queryIndex");
			job.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			// setting the query path by the user from the command to the Configuration			 
			Configuration configuration = job.getConfiguration();
			configuration.set("QueryPath",args[2]);

			return job.waitForCompletion(true)  ? 0 : 1;
		}
	  
	  public static class Map extends Mapper<Object,  Text ,  Text ,  Text > {
		  
		  
		  public void map( Object offset,  Text lineText,  Context context)
					throws  IOException,  InterruptedException {
			  
			  
			  ArrayList<String> queryLine = new ArrayList<String>();
			  
			  // extracting the query path given by the user 
			  Path pt = new Path(context.getConfiguration().get("QueryPath"));
			  
			  Configuration con = new Configuration(context.getConfiguration());
			  FileSystem fs = FileSystem.get(context.getConfiguration());
			  BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			  
			  String query = br.readLine();
			  while (query != null){
				  queryLine.add(query.trim());
				  query = br.readLine();
			  }
			  
			  br.close();


			// splitting the input based on the space
			  String[] s = lineText.toString().split(" ");
			  String word = s[0].trim();

			  for (String q: queryLine)
			  {
				  if(q.equals(word)){
					  context.write(new Text(word),new Text(s[2]));
				  }
				  
			  }
			  
		  }
	  }
	
	  
	  public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		  
		  public void reduce( Text word,  Text line,  Context context)
					throws IOException,  InterruptedException {
			 			  
			  context.write(word,line);  
		  }
	  }
}
