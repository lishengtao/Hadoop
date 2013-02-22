package cn.id5.hadoop.test.minimal;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.id5.hadoop.test.util.JobBuilder;

public class MinimalMapReduce extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = JobBuilder.parseInputAndOutput(this, this.getConf(), args);
		if(conf == null){
			return -1;
		}
		
		JobClient.runJob(conf);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
		System.exit(exitCode);
	}
}
