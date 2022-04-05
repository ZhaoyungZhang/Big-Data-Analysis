package example;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceMergeSort {
public static Configuration conf;
	
	public static void init() {
		conf=new Configuration();
		conf.set("fs.defaultFS","hdfs://localhost:9000");
		
	}
    //mapc 读取value，转化成IntWritable，最后输出key
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
        private static IntWritable data = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
        String line = value.toString();
        data.set(Integer.parseInt(line));
        context.write(data, new IntWritable(1));
        }
    }

    //reduce 将map输入key复制到输出的value上
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		//定义一个rank_num来代表key的位次
        private static IntWritable rank_num = new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
           for(IntWritable num : values) {
             context.write(rank_num, key);
             rank_num = new IntWritable(rank_num.get() + 1);
    		}
  		}
	}

    //Partition函数和
    public static class Partition extends Partitioner<IntWritable, IntWritable>{
        public int getPartition(IntWritable key, IntWritable value, int num_Partition){
            int Maxnumber = 65223;//int型的最大数值
            int bound = Maxnumber / num_Partition + 1; //根据输入数据的最大值
            int Keynumber = key.get();
			//将输入数据按照大小分块的边界，然后根据输入数值和边界的关系返回对应的Partiton ID
            for(int i = 0; i < num_Partition; i++){
              	if(Keynumber < bound * i && Keynumber >= bound * (i - 1)) {
                	return i - 1;
      			}
   			}
            return -1 ;
        }
    }

	public static void mergesort() throws Exception{
		init();
		String[] path=new String[]{"/user/hadoop/input/test2","/user/hadoop/output/test2"};
        if (path.length != 2) { 
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2); 
		}
        Job job = Job.getInstance(conf,"Merge and Sort");
        job.setJarByClass(MapReduceMergeSort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setPartitionerClass(Partition.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(path[0]));
        FileOutputFormat.setOutputPath(job, new Path(path[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        mergesort();
    }
}
