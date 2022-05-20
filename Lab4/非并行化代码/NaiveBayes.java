package lab4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

public class NaiveBayes {
	// 加载conf
	private static NaiveBayesConf NBConf = new NaiveBayesConf();
	// 保存频数
	public static HashMap<String,Integer> freq = new HashMap<String,Integer>();
	// add 
	public static void add(String key){
		// 不存在则初始化为1
		if(freq.get(key) == null)
			freq.put(key, new Integer(1));
		else{
			// 频数++
			int temp = freq.get(key).intValue() + 1;
			freq.put(key,new Integer(temp));
		}
	}
	
	/* Input	line
	 * Output	key:sum							value:count
	 * 			key:classStr					value:count
	 * 			key:classStr#deminNo#deminVal	value:count
	 */
	public static void train(String trainFile) throws Exception{
		FileInputStream filein = new FileInputStream(trainFile);
		BufferedReader bufin = new BufferedReader(new InputStreamReader(filein));
		// sum用来count 某分类结果的频数
		int sum = 0;
		String line;
		// 逐行扫描训练数据集
		while ((line = bufin.readLine()) != null) {
			sum++;
			String[] vals = line.split("\t");
			// add 类别
			add(vals[0]);
			//add 类别#属性编号#属性值
			for(int i = 1;i < vals.length;i++){
				String temp = new String();
				temp += vals[0] + "#" + (i - 1) + "#" + vals[i];
				add(temp);
			}
		}
		freq.put("sum",new Integer(sum));
		bufin.close();
		filein.close();
	}
	
	/*
	* 	Output	id	class_id
	*/
	public static void test(String testFile,String outFile) throws Exception{
		FileInputStream filein = new FileInputStream(testFile);
		BufferedReader bufin = new BufferedReader(new InputStreamReader(filein));
		
		FileOutputStream fileout = new FileOutputStream(outFile);
		BufferedWriter bufout = new BufferedWriter(new OutputStreamWriter(fileout));
		String line;
		// 读取测试集
		while ((line = bufin.readLine()) != null) {
			// P(Yi) P(X|Yi) P(Xj | Yi)
			double pyi,px_yi,pxj_yi;
			double maxp = 0;
			int idx = -1;
			
			int sum,sub;
			// get Sum
			Integer integer = freq.get("sum");
			if(integer == null) sum = 0;
			else sum = integer.intValue();
			//5	3.3	1.4	0.2 分割出 属性值
			String[] vals = line.split(" ");
			
			for(int i = 0;i < NBConf.class_num;i++){
				px_yi = 1;
				// get class name
				String class_name = NBConf.classNames.get(i);
				integer = freq.get(class_name);
				// 计算P(Yi)
				if(integer == null){
					sub = 0;
					pyi = 0;
				}
				else{
					sub = integer.intValue();
					pyi = (double)sub / sum;
				}
				// 计算P(Xj | Yi)
				for(int j = 1;j < vals.length;j++){
					String temp = class_name + "#" + (j - 1) + "#" + vals[j];
					integer = freq.get(temp);
					if (integer == null) pxj_yi = 0;
					else pxj_yi = (double)integer.intValue() / sub;
					px_yi = px_yi * pxj_yi;
				}
				// 计算最大的类别
				if(px_yi * pyi > maxp){
					maxp = px_yi * pyi;
					idx = i;
				}
			}
			// 写结果
			bufout.write(vals[0] + "\t" + idx + "\n");
		}
		bufin.close();
		filein.close();
		bufout.close();
		fileout.close();
	}
	// 计算准确率 读入测试结果
	public static void getAccuarcy(String outfile, String answerfile) throws Exception{
		FileInputStream filetest = new FileInputStream(outfile);
		BufferedReader tests = new BufferedReader(new InputStreamReader(filetest));
		FileInputStream fileans = new FileInputStream(answerfile);
		BufferedReader answers = new BufferedReader(new InputStreamReader(fileans));
		String test;
		String answer;
		// 读取测试的结果 和 标准结果比对
		int sum = 0;
		while ((test = tests.readLine()) != null && (answer = answers.readLine()) != null) {
			String[] testval = test.split("\t");
			String[] ansval = answer.split(" ");
			
			int testnum = Integer.parseInt(testval[1]);
			int ansnum = Integer.parseInt(ansval[1]);
			//System.out.println("test:"+ testnum + " answer:"+ ansnum + "\n");
			if(testnum == ansnum)
				sum++;
		}
		System.out.println("Accuarcy: " + sum / 30.0);
	}

	
	public static void main(String[] args){
		String path_conf = "/home/hadoop/irisData/iris.conf";
		String path_train = "/home/hadoop/irisData/input/";
		String path_test = "/home/hadoop/irisData/testdata";
		String path_out = "/home/hadoop/irisData/outSerial";
		String path_ans = "/home/hadoop/irisData/testanswer";
		
		try {
			File file = new File(path_train);
			String[] filelist = file.list();
			
			NBConf.ReadNaiveBayesConf(path_conf);
			long startTime=System.currentTimeMillis();   //获取开始时间  
			for(int i=0;i<filelist.length;i++) {
				String readfile = path_train+filelist[i];
				System.out.println(i);
				train(readfile);
			}
			long endTime=System.currentTimeMillis(); //获取结束时间  
			System.out.println("程序运行时间： "+(endTime-startTime)+"ms");   
			test(path_test,path_out);
			getAccuarcy(path_out,path_ans);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("success!");
	}
}
