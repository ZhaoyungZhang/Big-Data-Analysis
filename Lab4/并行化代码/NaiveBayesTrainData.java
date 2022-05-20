package Paralab4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NaiveBayesTrainData {
	// 记录频数
	public HashMap<String,Integer> freq;
	public NaiveBayesTrainData(){
		freq = new HashMap<String,Integer>();
	}
	public void getData(Configuration conf) throws IOException{
		// get 训练结果
		Path path = new Path(conf.get("train_result") + "/part-r-00000");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		// 诸行读取
		String line;
		while ((line = d.readLine()) != null) {
			String[] res = line.split("\t");
			// 更新freq 便于测试机使用
			freq.put(res[0], new Integer(res[1]));
		}
		
		
//		Path path2 = new Path(conf.get("train_result") + "/part-r-00001");
//		FileSystem fs2 = FileSystem.get(conf);
//		FSDataInputStream in2 = fs2.open(path);
//		BufferedReader d2 = new BufferedReader(new InputStreamReader(in2));
//		// 诸行读取
//		String line2;
//		while ((line2 = d2.readLine()) != null) {
//			String[] res = line2.split("\t");
//			// 更新freq 便于测试机使用
//			freq.put(res[0], new Integer(res[1]));
//		}
//		d2.close();
//		in2.close();
		d.close();
		in.close();
	}
}
