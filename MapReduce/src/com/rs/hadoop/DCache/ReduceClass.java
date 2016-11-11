package com.rs.hadoop.DCache;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurances in the output
 * 
 * @author Raman
 */
public class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{

	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		
		for(IntWritable valuesIt : values){
			sum = sum + valuesIt.get();
		}		
		context.write(key, new IntWritable(sum));
	}	
}