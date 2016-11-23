/*********************** 8.	Count of transactions for each store, department and type   ***********************/

package com.distributedcache.countOftransaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * Using ToolRunner to run the MapReduce program
 */
public class MapSideJoin extends Configured implements Tool {

  @Override
	public int run(String[] args) throws Exception {
	  /*
	   * Three arguments are required 
	   * 1. Input Directory where the large files are stored
	   * 2. Output Path where the output of the MR code will be saved
	   * 3. Distributed Cache file(comparatively smaller size), which would read the values and save in HashMap in Mapper
	   */
		if (args.length < 3) {	
			System.out.printf("Two parameters are required- <input dir> <output dir>\n");
			return -1;
		}
		Job job = new Job(getConf());		// Establish the MapReduce code with the MR configuration
		Configuration conf = job.getConfiguration(); // Getting the configuration of JobContext
		job.setJobName("Map-side join with text lookup file in DCache");
		
		DistributedCache.addCacheFile(new Path(args[2]).toUri(),conf);	// Add the small size file in the cache path of Distributed Cache
		
		job.setJarByClass(MapSideJoin.class);				// Set up the driver class and assign it in job
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));	// set the input path in job context
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // set the output path in job context
		
		job.setMapperClass(Map.class);							// set the mapper class in job context
		
		job.setReducerClass(Reduce.class);						// set the reducer class in job context
		
		job.setOutputKeyClass(Text.class);						// set the output key class for the reducer
		job.setOutputValueClass(IntWritable.class);				// set the output value class for the reducer
		job.setOutputFormatClass(TextOutputFormat.class);		// set the output format class for the reducer
		
		boolean success = job.waitForCompletion(true);			// returns a boolean value for the job to complete
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new MapSideJoin(), args);	// run the MR Code with tool runner
		System.exit(exitCode);
	}
}
/****	output
hduser@hadoop-master:~$ hdfs dfs -cat /tranop1/p*
16/11/10 11:30:00 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
10:1:B  3
1:1:A   4
2:1:A   3
3:1:B   4
4:1:A   3
5:1:B   1
6:1:A   2
7:1:B   2
8:1:A   3
9:1:B   3
*****/