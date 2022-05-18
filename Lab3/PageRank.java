package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class exp3 {
	
	/*
    **  GraphBuilder: 建立网页之间的超链接图
    *   只需要override map,reduce 使用默认
    */
	public static class GraphBuilder {
        /*
        **  GraphBuilderMapper: 逐行分析原始数据 
        *   输出 <URL, (PR_init, link_list)>
        */
		public static class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, Text> {
            @Override
			protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                // PR值初始化为1
                String PR = "1.0\t";  
                // 文本按照 \t 划分 [0]是pi  [1]是linklist
                String[] newvalue = value.toString().split("\t");
                Text page = new Text(newvalue[0]);
                PR += newvalue[1];
                // 这里使用 \t 把PR 和 LinkList分隔开
                context.write(page, new Text(PR));
			}
        }
        /*
        **  GraphBuilder.main 设置参数
        */
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            // set job
            Job job = Job.getInstance(conf, "GraphBuilder");
            job.setJarByClass(GraphBuilder.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(GraphBuilderMapper.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // 等待任务完成
            job.waitForCompletion(true);
        }
	}

    /*
    **  PageRankIter: 迭代计算各个网页的PR值
    *   直到PR值收敛或者迭代到指定次数
    */
    public static class PageRankIter {
        // damping
        private static final double d = 0.85;
        /*
        **  PRIterMapper
        *   output: <当前page i,连接到i的pagej \t 对i贡献的pr值>
        *   并传递图结构
        */
        public static class PRIterMapper extends Mapper<LongWritable, Text, Text, Text> {
        	@Override
        	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                // 注意这里的k,v 不是上一阶段GraphBuilder输出的kv而是从文件读的kv
                // 所以 value = <url,(pr \t link_list)> 分隔出
        		String[] temp = value.toString().split("\t");
                // temp[0]:当前网页 temp[1]:PR temp[2]:指向网页
                String pageKey = temp[0];
                double PR = Double.parseDouble(temp[1]);
                if (temp.length > 2) {
                    String[] link_list = temp[2].split(","); // 分割LinkList
                    for (String linkPage : link_list) {
                        context.write(new Text(linkPage), new Text(String.valueOf(cur_rank / link_list.length)));
                    }
                }
                // 传连接图结构 前面加一个标记#
                context.write(new Text(pageKey), new Text("#" + temp[2])); 
        	}
        }
        /*
        **  PRIterReducer
        *   intput: 多个<url,val> 和 一个<url,url_list>
        *   output: <url,(new_rank,url_list)>
        */
        public static class PRIterReducer extends Reducer<Text, Text, Text, Text> {
        	@Override
        	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        		double new_rank = 0;
                String url_list = "";
                for (Text value : values) {
                    String tmp = value.toString();
                    // 如果是图结构的链出信息
                    if (tmp.startsWith("#")) {
                        // urlist
                        url_list = tmp.substring(1);
                    } else {
                        // 计算newrank
                        new_rank += Double.parseDouble(tmp);
                    }
                }
                // 考虑dummy 不是 1-d / N
                new_rank = d * new_rank + (1 - d);
                // \t 作为分隔符
                context.write(key, new Text(String.valueOf(new_rank)+"\t"+url_list));
        	}
		}
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRankIter");
            job.setJarByClass(PageRankIter.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(PRIterMapper.class);
            job.setReducerClass(PRIterReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
    }
    
    /*
    **  PageRankViewer: 将PageRank值从大到小输出
    */
    public static class PageRankViewer {
    	/*
        **  PRViewerMapper: 从前面最后一次迭代的结果中读出PageRank值和文件名,并以pr值作为key,网页名称作为value,输出键值对<PageRank, URL>。
        */
    	public static class PRViewerMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    		@Override
    		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                // value = <url,(new_rank,url_list)>
    			String[] tmp = value.toString().split("\t");
                context.write(new DoubleWritable(Double.parseDouble(tmp[1])), new Text(tmp[0]));
    		}
    	}
    	public static class DescDoubleComparator extends DoubleWritable.Comparator {
    		public float compare(WritableComparator a, WritableComparable<DoubleWritable> b) {
    			return -super.compare(a, b);
    		}
    		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    			return -super.compare(b1, s1, l1, b2, s2, l2);
    		}
    	}
    	public static class PRViewerReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    		@Override
    		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    			for (Text value : values) {
    				context.write(new Text("(" + value + "," + String.format("%.10f", key.get()) + ")"), null);
                }
    		}
    	}
        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "PageRankViewer");
            job.setJarByClass(PageRankViewer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(PRViewerMapper.class);
            job.setSortComparatorClass(DescDoubleComparator.class);
            job.setReducerClass(PRViewerReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
    }
    
    public static class PageRankDriver {
    	private static int times = 10;
    	public static void main(String[] args) throws Exception {
			String[] forGB = {"", args[1] + "/Data0"};  // GraphBuilder
			forGB[0] = args[0];
			GraphBuilder.main(forGB);
			String[] forItr = {"", ""};  // PageRankIter
			for (int i = 0; i < times; i++) {
				forItr[0] = args[1] + "/Data" + i;
				forItr[1] = args[1] + "/Data" + (i + 1);
				PageRankIter.main(forItr);
			}
			String[] forRV = {args[1] + "/Data" + times, args[1] + "/FinalRank"};  // PageRankViewer
			PageRankViewer.main(forRV);
		}
    }
    
    // 主函数入口
    public static void main(String[] args) throws Exception {
    	PageRankDriver.main(args);
    }
}