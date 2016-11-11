package com.rs.hadoop.CPart;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//mapper output format : gender is the key, the value is formed by concatenating the name, age and the score

// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type
   
public class PartitionMapper extends Mapper<Object, Text, Text, Text> {

   
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");

        String gender = tokens[2].toString();
        String nameAgeScore = tokens[0]+"\t"+tokens[1]+"\t"+tokens[3];
       
        //the mapper emits key, value pair where the key is the gender and the value is the other information which includes name, age and score
        context.write(new Text(gender), new Text(nameAgeScore));
    }
}