package example;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import example.MapReduceMergeSort.Map;
import example.MapReduceMergeSort.Partition;
import example.MapReduceMergeSort.Reduce;
public class InformationMiningTest {
	
	public static Configuration conf;
	public static int WriteTableHeadTime = 0;
	public static void init() {
		conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		//WriteTableHeadTime = 0;
	}
    public static class Map extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			String line = value.toString();
			// 分别得到child和parent
			String[] childAndParent = line.split(" ");
			List<String> list = new ArrayList<>(2);
			// 加到list里面
			for (String childOrParent : childAndParent) {
				if (!"".equals(childOrParent)) {
					list.add(childOrParent);
				} 
			} 
			if (!"child".equals(list.get(0))) {
				String childName = list.get(0);
				String parentName = list.get(1);
				String relationType = "1";
				// 父子
				context.write(new Text(parentName), new Text(relationType + "+"
					+ childName + "+" + parentName));
				relationType = "2";
				// 子当作父
				context.write(new Text(childName), new Text(relationType + "+"
					+ childName + "+" + parentName));
			}
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
			//输出表头
        	if(WriteTableHeadTime == 0) {
        		context.write(new Text("grandchild"), new Text("grandparent"));
        		WriteTableHeadTime++;
        	}
            //获取value-list中value的child
            List<String> grandChild = new ArrayList<>();
            //获取value-list中value的parent
            List<String> grandParent = new ArrayList<>();
			//左表，取出child放入grand_child
            for (Text text : values) {
                String s = text.toString();
                String[] relation = s.split("\\+");
                String relationType = relation[0];
                String childName = relation[1];
                String parentName = relation[2];
                if ("1".equals(relationType)) {
                    grandChild.add(childName);
                } else {
                    grandParent.add(parentName);
                }
            }

			//右表，取出parent放入grand_parent
			int grandParentNum = grandParent.size();
			int grandChildNum = grandChild.size();
			if (grandParentNum != 0 && grandChildNum != 0) {
			for (int m = 0; m < grandChildNum; m++) {
				for (int n = 0; n < grandParentNum; n++) {
					//输出结果
				context.write(new Text(grandChild.get(m)), new Text(
							grandParent.get(n)));
					}
				}
			}
        } 
    }
	
	public static void InformationMining() throws Exception{
		init();
		String[] path=new String[]{"/user/hadoop/input/test3","/user/hadoop/output/test3"};
        if (path.length != 2) { 
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2); 
		}
        Job job = Job.getInstance(conf,"Single table join");
        job.setJarByClass(InformationMiningTest.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(path[0]));
        FileOutputFormat.setOutputPath(job, new Path(path[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
    	InformationMining();
    }
	
}
