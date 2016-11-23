package com.distributedcache.countOftransaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/* Using Distributed Cache to count no. of transactions w.r.t sales store
 * 
 * 
 */
public class Map extends Mapper<LongWritable, Text, Text,IntWritable> {

	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();		// HashMap to define <key,value> from the Distributed Cache file
	private BufferedReader brReader;
	private String store = "";
	private Text storeKey = new Text("");
	private Text txtMapOutputKey = new Text("");
	private final static IntWritable one = new IntWritable(1);					// value for each transaction record
	@Override
	protected void setup(Context context) throws IOException, 
	InterruptedException {
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());	// opens an Path array where all the distributed cache files would be stored
		if (cacheFilesLocal != null && cacheFilesLocal.length > 0) {
			for (Path eachPath : cacheFilesLocal) {
				loadDepartmentsHashMap(eachPath, context);			// read each file from the cacheFilesLocal Path Array and loads <key,value> in HashMap
			}
		}
	}
	private void loadDepartmentsHashMap(Path filePath, Context context) throws IOException {
		try {
			String strLineRead = null;
			brReader = new BufferedReader(new FileReader(filePath.toString()));	// opens a buffered reader for reading the file
			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
				String deptFieldArray[] = strLineRead.split("\t");						// split each line with a delimiter("\t") and save the values in and array 
				DepartmentMap.put(deptFieldArray[0].trim(),deptFieldArray[1].trim());	// put the above values in the HashMap 
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String content = value.toString();					// convert the whole data from input into a string
		String line[] = content.split("\n");				// split the above data with newline and saves each line in an array
		for(int i = 0; i< line.length; i++ ){
			String arrEmpAttributes[] = line[i].split("\t");	// split each line according to the delimiter and saves in array
			try{
				store = DepartmentMap.get(arrEmpAttributes[0]);		// save the value from HashMap and store in a variable
				
				storeKey.set(arrEmpAttributes[0].toString()+":"+arrEmpAttributes[1].toString()+":"+store); // set the key of the Mapper
			} 
			finally{
			//	storeKey = ((storeKey.equals(null) || storeKey.equals("")) ? "NOT-FOUND" : storeKey);
			}
			txtMapOutputKey.set(storeKey);
			
		}
		context.write(txtMapOutputKey, one);				// set the output context with the key and transaction count as 1, this output will go as input to the reducer
	}
}

