package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Counter2 {

 	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{


    private int node_value;
    private Text inbound_node = new Text();
    private IntWritable output = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] fields = value.toString().split("\t");
      inbound_node.set(fields[1]);
      node_value = Integer.parseInt(fields[2]);
      output.set(node_value);
      context.write(inbound_node, output);
    }
  }

  	public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int next = 10000000;
      for (IntWritable val : values) {
        if (val.get() < next){
          next = val.get();
        }
      }
      result.set(next);
      context.write(key, result);
    }
  }



 	public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Counter2");

    	job.setJarByClass(Counter2.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setCombinerClass(IntSumReducer.class);
      job.setReducerClass(IntSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
