package com.rs.hadoop.DCache;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a single line at a time, it splits the line based on space
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * @author Raman
 */
public class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
	 
	private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @SuppressWarnings("rawtypes")
	private HashSet stopWords = new HashSet();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	try{
    		Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    		if(stopWordsFiles != null && stopWordsFiles.length > 0) {
    			for(Path stopWordFile : stopWordsFiles) {
    				readFile(stopWordFile);
    			}
    		}
    	} catch(IOException ex) {
    		System.err.println("Exception in mapper setup: " + ex.getMessage());
    	}
    }
    
    /**
     * map function of Mapper parent class takes a line of text at a time
     * splits to tokens and passes to the context as word along with value as one
     */
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
	
//		String line = value.toString();
//        StringTokenizer st = new StringTokenizer(line,",");
//        while(st.hasMoreTokens()){
//            String wordText = st.nextToken();         
//            if(stopWords.contains(wordText.toLowerCase())) {  
//                word.set(wordText);
//                context.write(word,one);
//            }
//        }
		String line = value.toString();
//		StringTokenizer st = new StringTokenizer(line," ");
		String[] st = line.split(",");
		
		for(int i=0; i<st.length; i++){
			if(!stopWords.contains(st[i].toLowerCase())){
				word.set(st[i]);
				context.write(word, one);
			}
		}		
	}
	
	@SuppressWarnings("unchecked")
	private void readFile(Path filePath) {
		try{
			@SuppressWarnings("resource")
			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
			String stopWord = null;
			while((stopWord = bufferedReader.readLine()) != null) {
				stopWords.add(stopWord.toLowerCase());
			}
		} catch(IOException ex) {
			System.err.println("Exception while reading stop words file: " + ex.getMessage());
		}
	}
}