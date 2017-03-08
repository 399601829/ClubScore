package cn.edu.bistu.lcy.club.score;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;


/**
 * 操作和操作对象的二维数组
 * @author NarutoKu
 *
 */
public class ScoreMatrix {

	/**
	 * 在一个java虚拟机中只能调用一次，因此将其放置在静态代码块中执行
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	/**
	 * 使用Hadoop URL读取数据
	 * @param filePath
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static synchronized double[][] getMatrix(String filePath) throws MalformedURLException, IOException {
		final int ROWS_NUM = new ScoreMatrix().getRowsNum(filePath);
		final int COLS_NUM = new ScoreMatrix().getColsNum(filePath);
		InputStream in = null;
		in = new URL(filePath).openStream();
		InputStreamReader reader = new InputStreamReader(in);
		BufferedReader bf = new BufferedReader(reader);
		
		double[][] scoreMatrix = new double[ROWS_NUM][COLS_NUM];
		for(int row = 0;row<ROWS_NUM;row++) {
 			String[] scores = bf.readLine().split(",");
			for(int col = 0;col < COLS_NUM;col++) {
				scoreMatrix[row][col] = Double.parseDouble(scores[col]);
			}
		}
		bf.close();
		reader.close();
		in.close();
		return scoreMatrix;
	}
	
	public synchronized int getRowsNum(String filePath) throws IOException {
		InputStream in = null;
		in = new URL(filePath).openStream();
		InputStreamReader reader = new InputStreamReader(in);
		BufferedReader bf = new BufferedReader(reader);
		
		int rowsNum;
		for(rowsNum = 0;bf.readLine() != null;rowsNum++);
		
		bf.close();
		reader.close();
		in.close();
		return rowsNum;
	}
	
	public synchronized int getColsNum(String filePath) throws IOException {
		InputStream in = null;
		in = new URL(filePath).openStream();
		InputStreamReader reader = new InputStreamReader(in);
		BufferedReader bf = new BufferedReader(reader);
		
		int colsNum = bf.readLine().split(",").length;
		
		bf.close();
		reader.close();
		in.close();
		return colsNum;
	}
	
}
