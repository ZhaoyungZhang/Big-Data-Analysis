package lab4;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class NaiveBayesConf {
	// 种类数目
		public int class_num;
		// 种类名字
		public ArrayList<String> classNames;
		// 维度
		public int dimen;
		// 
		public ArrayList<String> attribute;
		// 初始化conf
		public NaiveBayesConf(){
			class_num = 0;
			classNames = new ArrayList<String>();
			dimen = 0;
			attribute = new ArrayList<String>();
		}
		// 读取Conf文件
		public void ReadNaiveBayesConf(String confFile) throws Exception{
			// Conf File
			FileInputStream filein = new FileInputStream(confFile);
			// 放入缓冲区读取
			BufferedReader buf = new BufferedReader(new InputStreamReader(filein));
			// readline
			String line = buf.readLine();
			// 3 Iris-setosa Iris-versicolor Iris-virginica
			String[] vals = line.split(" ");
			// get class num
			class_num = Integer.parseInt(vals[0]);
			// store class Name
			for(int i = 1;i < vals.length;i++)
				classNames.add(vals[i]);
			// 4 SepalLengthCm	SepalWidthCm	PetalLengthCm	PetalWidthCm
			line = buf.readLine();
			vals = line.split(" ");
			// get dimension = 4
			dimen = Integer.parseInt(vals[0]);
			// add attribute name
			for(int i = 1;i < vals.length;i += 1)
				attribute.add(vals[i]);
			buf.close();
			filein.close();
		}
}
