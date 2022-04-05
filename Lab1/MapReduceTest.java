package example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class MapReduceTest {
	public static Configuration conf;
	
	public static void init() {
		conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		
	}
	    //重载map函数 直接将输入中的value复制到输出数据的key上 注意在map方法中要抛出异常：throws IOException,InterruptedException
	    public static class Map extends Mapper<Object, Text, Text, Text> {  
	        private static Text text = new Text();
	        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {  
	            text = value;  
	            content.write(text, new Text(""));  
	        }  
	    }  
	    //重载reduce函数 直接将输入中的key复制到输出数据的key上  注意在reduce方法上要抛出异常：throws IOException,InterruptedException
        public static class Reduce extends Reducer<Text, Text, Text, Text> {  
	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
	            context.write(key, new Text(""));  
	        }  
	    }

		public static void testMerge() throws IOException, ClassNotFoundException, InterruptedException{
			init();
			String[] path=new String[]{"/user/hadoop/input/test1","/user/hadoop/output/test1/C.txt"};
			if(path.length!=2){
				System.err.println("Usage:Merge and duplicate removal<in><out>");
				System.exit(2);
			}
			Job job=Job.getInstance(conf,"Merge and duplicate removal");
			job.setJarByClass(MapReduceTest.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job,new Path(path[0]));
			FileOutputFormat.setOutputPath(job,new Path(path[1]));
			System.exit(job.waitForCompletion(true)?0:1);
		}
		public static void main(String[] args) throws Exception{
			// 测试合并
			testMerge();
		}
		
}
