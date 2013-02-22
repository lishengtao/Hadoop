package cn.id5.hadoop.test.v1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperature {

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MaxTemperature.class);
		conf.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(conf, new Path("/hdg/1901"));
		FileOutputFormat.setOutputPath(conf, new Path("/hdg/max"));
		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setReducerClass(MaxTemperatureReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(conf);
	}

}
