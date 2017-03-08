package cn.edu.bistu.lcy.club.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;



public class TestHadoopAPI {
	/**
	 * 在一个java虚拟机中只能调用一次，因此将其放置在静态代码块中执行
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	
	public static void main(String argsp[]) throws MalformedURLException, IOException {
		
		final String FILE_URL = "hdfs://192.168.1.10:9000/user/hadoop/test.txt";
		final int ROWS_NUM = new TestHadoopAPI().getRowsNum(FILE_URL);
		final int COLS_NUM = new TestHadoopAPI().getColsNum(FILE_URL);
		
		InputStream in = null;
		in = new URL(FILE_URL).openStream();
		InputStreamReader reader = new InputStreamReader(in);
		BufferedReader bf = new BufferedReader(reader);
		double[][] scoresRect = new double[ROWS_NUM][COLS_NUM];
		for(int row = 0;row<ROWS_NUM;row++) {
 			String[] scores = bf.readLine().split(",");
			for(int col = 0;col < COLS_NUM;col++) {
				scoresRect[row][col] = Double.parseDouble(scores[col]);
				System.out.print("   "+scoresRect[row][col]);
			}
			System.out.println("");
		}
	}
	public int getRowsNum(String filePath) throws IOException {
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
	public int getColsNum(String filePath) throws IOException {
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
