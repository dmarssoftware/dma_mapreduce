package com.rs.hadoop.DCache.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	private BufferedReader brReader;
	private String strDeptName = "";
	private Text txtMapOutputKey = new Text("");
	private Text txtMapOutputValue = new Text("");

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		if (cacheFilesLocal != null && cacheFilesLocal.length > 0) {
			for (Path eachPath : cacheFilesLocal) {
				loadDepartmentsHashMap(eachPath, context);
			}
		}
	}

	private void loadDepartmentsHashMap(Path filePath, Context context) throws IOException {

		try {
			String strLineRead = null;
			brReader = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = brReader.readLine()) != null) {
			
				String deptFieldArray[] = strLineRead.split("\t");
				
				DepartmentMap.put(deptFieldArray[0].trim(),deptFieldArray[1].trim());
			
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String content = value.toString();
		String line[] = content.split("\n");
		
		for(int i = 0; i< line.length; i++ ){
			
			String arrEmpAttributes[] = line[i].split("\t");

			try {
				strDeptName = DepartmentMap.get(arrEmpAttributes[6].toString());
			} finally {
				strDeptName = ((strDeptName.equals(null) || strDeptName
						.equals("")) ? "NOT-FOUND" : strDeptName);
			}

			txtMapOutputKey.set(arrEmpAttributes[0].toString());

			txtMapOutputValue.set(strDeptName);
		}
		context.write(txtMapOutputKey, txtMapOutputValue);
	}
}