package cn.edu.bistu.lcy.club.score;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalScoreMinMaxNormalizationMapper extends Mapper<LongWritable ,Text, IntWritable, Text> {
	
	/**
	 * 7：userId
	 * 8：操作代码
	 * 9：操作对象
	 * 10：对象Id
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split(","); 
		if(fields.length > 0) {
			context.write(new IntWritable(1),value);
		}
	}
}