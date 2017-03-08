package cn.edu.bistu.lcy.club.score;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ScoreVectorToFinalScoreVectorReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    @Override
    public void reduce(Text userAndObject, Iterable<DoubleWritable> scoreVector,Context context) throws IOException, InterruptedException{
    	double sumScore = 0;
        for(DoubleWritable scorevector : scoreVector){
        	sumScore += scorevector.get();
        }
        context.write(userAndObject,new DoubleWritable(sumScore));
    }
}