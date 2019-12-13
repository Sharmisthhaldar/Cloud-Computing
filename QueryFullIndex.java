package org.myorg;

/*
 * Sharmistha Haldar
 * shaldar@uncc.edu
 */

import java.io.*;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class QueryFullIndex extends Configured implements Tool{

	private static final Logger LOG = Logger.getLogger(QueryFullIndex.class);
	
	  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(new QueryFullIndex(), args);
		    System.exit(res);
		  }	

	  public int run( String[] args) throws  Exception {
			Job job  = Job.getInstance(getConf(), "queryFullIndex");
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
			configuration.set("InputFolder",args[3]);

			return job.waitForCompletion(true)  ? 0 : 1;
		}
	  
	  public static class Map extends Mapper<Object,  Text ,  Text ,  Text > {
		  
		  String searchFile;
		  protected void setup(Mapper.Context context) {
			  searchFile = context.getConfiguration().get("InputFolder");
		  }
		  
		  public void map( Object offset,  Text lineText,  Context context)
					throws  IOException,  InterruptedException{
			  
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
			  String[] s = lineText.toString().split("\\s+");
			  String word = s[0].trim();
			  
			  for (String q: queryLine)
			  {
				  if(q.equals(word)){
					  
					  // Separating the list of files into array string
					  String [] str = s[2].split("\\+");
					  StringBuilder sb = new StringBuilder();
					  
					  for(String s1: str){
						  String temp[] = s1.split("@");
						  String fname = searchFile + temp[0];
						  int loc = Integer.parseInt(temp[1]);	
						  
						  Path p = new Path(fname);

						  FSDataInputStream in = fs.open(p);
						  in.seek(loc);
						  
						  String result = in.readLine();
						  sb.append("\n");
						  sb.append(s1);
						  sb.append("->");
						  sb.append(result);
						  
						  in.close();
						  
					  }
					  
					  context.write(new Text(word + " :"),new Text(sb.toString()));
					  
				  }
				  
			  }			  
			  
		  }
	  }
	  
	  public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text >{
		  
		  public void reduce( Text word,  Text line,  Context context)
					throws IOException,  InterruptedException {
			  
			  context.write(word,line);  
			 			  
		  }
	  }
}
