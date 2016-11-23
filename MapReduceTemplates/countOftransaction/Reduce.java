package com.distributedcache.countOftransaction;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/*
 * Only summation of transaction count is occurring in reducer 
 */
public class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{ // <InputKey,InputValue,OutputKey,OutputValue>
	/*
	 * Output of Mapper comes here as Text format key and its value (1) for each record
	 */
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value : values)
		{
			sum += value.get();								// Sum of Values for each key in each loop
		}
		context.write(key, new IntWritable(sum));			// Write the key and summation of the values and saves it as output

	}
}