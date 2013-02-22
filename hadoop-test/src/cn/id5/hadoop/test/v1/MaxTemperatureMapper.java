package cn.id5.hadoop.test.v1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int MISSING = 9999;
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperatue;
		if(line.charAt(87) == '+'){
			airTemperatue = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperatue = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if(airTemperatue != MISSING && quality.matches("[01459]")){
			outputCollector.collect(new Text(year), new IntWritable(airTemperatue));
		}
	}

}
