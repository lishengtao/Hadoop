package cn.id5.hadoop.test.minimal;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.id5.hadoop.test.util.JobBuilder;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = JobBuilder.parseInputAndOutput(this, this.getConf(), args);
		if(conf == null){
			return -1;
		}
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setNumMapTasks(1);
		conf.setMapperClass(IdentityMapper.class);
		conf.setMapRunnerClass(MapRunner.class);
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setPartitionerClass(HashPartitioner.class);
		conf.setNumReduceTasks(1);
		conf.setReducerClass(IdentityReducer.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		JobClient.runJob(conf);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
		System.exit(exitCode);
	}
}
