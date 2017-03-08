package cn.edu.bistu.lcy.club.score;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class FinalScoreMinMaxNormalizationReducer extends Reducer<IntWritable, Text, Text, Text>{
    @Override
    public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
    	
    	double max = 0;
    	double min = 0;
    	int count = 0;
    	double score = 0;
    	// TODO 可完善 大小应该为用户数 * 社团数
    	String[] users = new String[1000];
    	String[] clubs = new String[1000];
    	double[] scores = new double[1000];
    	for(Text value : values) {
    		String line = value.toString();
    		String[] fields = line.split(",");
    		String userId = fields[0];
    		String clubId = fields[1];
    		score = Double.parseDouble(fields[2].toString());
    		if(count == 0) {
    			max = min = score;
    		}
    		if(max < score) {
    			max = score;
    		}
    		if(score < min) {
    			min = score;
    		}
    		users[count] = userId;
    		clubs[count] = clubId;
    		scores[count] = score;
    		++count;
    	}
    	for(int i = 0;i < count;i++) {
    		String userAndClub = users[i] + "," + clubs[i];
    		double mormalizationScore = (double)(((scores[i] - min)/(max - min)) * 5); 
    		String mormalizationScoreStr = String.format("%.2f", mormalizationScore);
    		context.write(new Text(userAndClub),new Text(mormalizationScoreStr));
    	}
    }
}