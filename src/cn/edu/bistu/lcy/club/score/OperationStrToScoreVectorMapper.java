package cn.edu.bistu.lcy.club.score;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OperationStrToScoreVectorMapper extends Mapper<LongWritable ,Text, Text, DoubleWritable> {
	
	/**
	 * 0: userId
	 * 1：操作代码
	 * 2：操作对象
	 * 3：对象Id
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void map(LongWritable key, Text operationVector, Context context) throws IOException, InterruptedException {
		// 得到用户操作向量字符串
		String line = operationVector.toString();
		String[] fields = line.split(",");
		Long userId = Long.parseLong(fields[0]);
		int operationCode = Integer.parseInt(fields[1]);
		int operationObject = Integer.parseInt(fields[2]);
		Long ObjectId = Long.parseLong(fields[3]);
		// 得到分值二维数组
		double[][] scoreMatrix = ScoreMatrix.getMatrix("hdfs://192.168.1.10:9000/user/hadoop/WebApp/Club/score.txt");
		// 得到得分向量
		double scoreVector = scoreMatrix[operationCode][operationObject];
		context.write(new Text(userId + "," + ObjectId), new DoubleWritable(scoreVector));
	}
}