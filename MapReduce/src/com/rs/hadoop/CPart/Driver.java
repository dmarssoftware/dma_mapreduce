package com.rs.hadoop.CPart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int res= ToolRunner.run(new Configuration(),new Driver(),args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        if(args.length!=2){
            System.out.print("Run as -- hadoop jar /path/to/partitioner.jar /inputdataset /output");
            System.exit(-1);
        }
         
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
         
        //Set number of reducer tasks
        job.setNumReduceTasks(3);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
         
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
         
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);
 
        //Set Partitioner Class
        job.setPartitionerClass(AgePartitioner.class);
         
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
