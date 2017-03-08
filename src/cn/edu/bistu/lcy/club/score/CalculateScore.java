package cn.edu.bistu.lcy.club.score;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
/**
 * 组装各个作业组件，完成推荐作业
 */
// TODO Configured和Tool分别是什么
public class CalculateScore extends Configured implements Tool{
	
	/**
	 * 在一个java虚拟机中只能调用一次，因此将其放置在静态代码块中执行
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new CalculateScore(),args);
        System.exit(returnCode);
   }
    @Override
    public int run(String[] args) throws Exception {
    	// 基本数据过滤
        @SuppressWarnings("deprecation")
		Job getOperationJob = new Job(getConf());
        getOperationJob.setJarByClass(CalculateScore.class);
        getOperationJob.setJobName("getOperationJob");
        
        getOperationJob.setOutputKeyClass(Text.class);
        getOperationJob.setOutputValueClass(Text.class);
        
        getOperationJob.setMapperClass(SourceDataToOperationVectorMapper.class);
        
        getOperationJob.setInputFormatClass(TextInputFormat.class);
        getOperationJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(getOperationJob,new Path("/user/hadoop/flume/spoolDir/clubSpool/logs"));
        FileOutputFormat.setOutputPath((JobConf) getOperationJob.getConfiguration(), new Path("/user/hadoop/WebApp/Club/Score/TmpOutput1"));
        // 设置输出分隔符为,
        Configuration getOperationJobConf = getOperationJob.getConfiguration();
        getOperationJobConf.set("mapred.textoutputformat.separator", ",");
        
        
        // 得到累计评分
        @SuppressWarnings("deprecation")
		Job getFinalScoreVectorJob = new Job(getConf());
        getFinalScoreVectorJob.setJarByClass(CalculateScore.class);
        getFinalScoreVectorJob.setJobName("getFinalScoreVectorJob");
          
        getFinalScoreVectorJob.setOutputKeyClass(Text.class);
        getFinalScoreVectorJob.setOutputValueClass(DoubleWritable.class);
        
        getFinalScoreVectorJob.setMapperClass(OperationStrToScoreVectorMapper.class);
        getFinalScoreVectorJob.setReducerClass(ScoreVectorToFinalScoreVectorReducer.class);
        
        getFinalScoreVectorJob.setInputFormatClass(TextInputFormat.class);
        getFinalScoreVectorJob.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(getFinalScoreVectorJob,new Path("/user/hadoop/WebApp/Club/Score/TmpOutput1"));
        FileOutputFormat.setOutputPath((JobConf) getFinalScoreVectorJob.getConfiguration(), new Path("/user/hadoop/WebApp/Club/Score/TmpOutput2"));
        // 设置输出分隔符为,
        Configuration FinalScoreVectorJobConf = getFinalScoreVectorJob.getConfiguration();
        FinalScoreVectorJobConf.set("mapred.textoutputformat.separator", ",");
        
        // min-max标准化
        @SuppressWarnings("deprecation")
		Job minMaxNormalization = new Job(getConf());
        minMaxNormalization.setJarByClass(CalculateScore.class);
        minMaxNormalization.setJobName("minMaxNormalization");
          
        
        minMaxNormalization.setMapOutputKeyClass(IntWritable.class);
        minMaxNormalization.setMapOutputValueClass(Text.class);
        
        minMaxNormalization.setOutputKeyClass(Text.class);
        minMaxNormalization.setOutputValueClass(Text.class);
        
        minMaxNormalization.setMapperClass(FinalScoreMinMaxNormalizationMapper.class);
        minMaxNormalization.setReducerClass(FinalScoreMinMaxNormalizationReducer.class);
        
        minMaxNormalization.setInputFormatClass(TextInputFormat.class);
        minMaxNormalization.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(minMaxNormalization,new Path("/user/hadoop/WebApp/Club/Score/TmpOutput2"));
        FileOutputFormat.setOutputPath((JobConf) minMaxNormalization.getConfiguration(), new Path("/user/hadoop/WebApp/Club/CF/item/input"));
        // 设置输出分隔符为,
        Configuration minMaxNormalizationConf = minMaxNormalization.getConfiguration();
        minMaxNormalizationConf.set("mapred.textoutputformat.separator", ",");
        
        //串联各个job
        if(getOperationJob.waitForCompletion(true)) {
        	if(getFinalScoreVectorJob.waitForCompletion(true)) {
        		return minMaxNormalization.waitForCompletion(true)? 1 :0;
        	}else {
        		throw new Exception("计算最终评分(未标准化)出错");
        	}
        }else{
        	throw new Exception("计算操作评分出错");
        }
    }
}