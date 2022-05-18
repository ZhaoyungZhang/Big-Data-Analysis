# Learn Page Rank

> PageRank是一种在搜索引擎中根据网页之间相互的链接关系计算网页排名的技术。
>
> 利用MapReduce 以及 Spark 分别实现PageRank

## 基本思想

一个网页如果比较重要，则需要有很多网页连接到他，有很多高质量的网页连接到他，自然而然，提出了“重要性”的传播

## 简化模型

可以把互联网上的各个网页之间的链接关系看成一个有向图。对于任意网页$P_i$，他的PageRank值可表示为$R(P_i) = \sum_{P_j \in B_i} \frac{R(P_j)}{L_j}$，$B_i$为所有链接到网页i的网页集合(指向自己的边)，$L_j$为网页J的对外链接(指向外的边)

对于上面的公式 理解大致如下

==**Pj —> Pi & Pj —>Lj**== 对于Pi来说Pj是连接到Pi的网页，而Lj是Pj指向的其他网页，因为重要性需要均分，所以除Lj(个数)

---

定义一个超链接的矩阵$H$

$ \mathbf{H}_{i j}=\left\{\begin{array}{cl}1 / L_{j} & \text { if } P_{j} \in B_{i} \\ 0 & \text { otherwise }\end{array}\right. $ 

比如$H_{12}$标识Page2 给 Page1 的重要性贡献

然后初值的话 R(Pi) = 1 每个网页的R都是1

$R = [R(P_i)],然后迭代,R = HR$ 最后得到矩阵R，然后根据此对网页进行排序

R是特征向量(对应于H的特征值为1)

## 存在问题

> 针对PageRank而言，需要图是强连通的才能满足收敛性。
>
> 但是现实中的网页可能存在终止点和陷阱

### Rank Leak

一个独立的网页如果没有外出的链接就产生排名泄漏，即**终止点**

解决办法：

1.将无出度的节点递归地从图中去掉，待其他节点计算完毕后再加上

2.对无出度的节点添加一条边，指向那些指向它的顶点

### Rank Sink

整个网页图中若有网页没有入度链接，导致其他构成强连通分量的网页们会吞噬其贡献，排名下沉，节点A的PR值会在迭代后趋向于0

但是其实也对，因为就是有的网页没有被引用嘛，但这样的话 绝大多数网页都是0了，所以还是要解决这个问题

## 随机浏览模型

> 不唯link论，考虑随机访问

简单模型的矩阵表示为 R = HR

**随机浏览模型**：

**图表示**上任意两个顶点之间都有直接通路，并且是分为**蓝色和红色路径**，蓝色路径表示按照超链接访问，也就是原来DAG图的边，而红色路径则是补充了一个完全图(带自环)，红色路径表示随机转移的概率。

<img src="https://vvtorres.oss-cn-beijing.aliyuncs.com/image-20220504193035531.png" alt="image-20220504193035531" style="zoom: 50%;" />

**公式**:$H’ = d*H + (1-d)*[\frac{1}{N}]_{N*N},\quad R = H'R$ ，其中以d概率按照超链接进行浏览，1-d的概率进行随即浏览一个新网页。d 对应 转移矩阵是H，而1-d对应的矩阵是[$\frac{1}{N}$]，N为所有网页个数，所以是按照一个均值的概率进行跳转。

> R=HR满足马尔可夫链的性质，如果马尔可夫链收敛，则R存在唯一解

网页数目巨大，网页之间的连接关系的邻接矩阵是一个很大的稀疏矩阵，采用**邻接表而非邻接矩阵**来表示网页之间的连接关系，

最终==**随机浏览模型的PageRank公式**==

$P R\left(p_{i}\right)=\frac{1-d}{N}+d \sum_{p_{j} \in M\left(p_{i}\right)} \frac{P R\left(p_{j}\right)}{L\left(p_{j}\right)}$

意味着某个网页pi,他的PR值 是通过随即浏览1-d / n 以及 超链接索引的贡献之和

通过迭代计算得到所有节点的PageRank值。

## MapReduce 编程实现

### GraphBuilder

分析原始数据，建立各个网页之间的链接关系，建立超链接图

原始数据 是 URLi, urlj in link_list 

- Map：逐行分析原始数据, 输出<URL ,(PR_init, link_list)>
  - 其中网页的URL作为key, PageRank初始值（PR_init）和网页的出度列表一起作为value,以字符串表示value，用特定的符号将二者分开。
- Reduce：输出<URL, (PR_init, link_list)>，该阶段的Reduce不需要做任何处理

```java
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
```



### PageRanklter

迭代计算PR值，直到PR值收敛或者迭代到计划的次数

**Map**对上阶段的 <URL, (cur_rank, link_list)>产生两种<key, value>对

**Output: \<url u, pr\> 以及传递 \<url,link_list\>**

1. For each *u* in link_list, 输出 <*u*, cur_rank / |link_list|>
   - 其中u代表当前URL所链接到网页ID，并作为key，cur_rank为当前URL的PageRank值，|Link_List|为当前URL的出度数量，cur_rank / |L| 作为value
2. 为了完成迭代过程，需要传递每个网页的链接信息<URL, link_list>。在迭代过程中，必须保留网页的局部链出信息，以维护图的结构

**Reduce** 对map输出的\<URL,url_list\> 和 多个 \<URL,value\> 做如下处理

- \<URL,URL_LIST\>是图结构，当前URL的链出信息
- \<URL,value\>是当前URL的链入网页对其贡献的PR值，需要计算所有value之和，然后通过d 得到新的new rank

**output:(URL,(new_rank,url_list))**

```java
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
```

### RankViewer 

```java
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
```

### PageRankDriver

```java
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
```

## Scala 编程实现

使用scala编程，并且采用sbt打包成jar包提交，

Scala源码以及注释如下

```scala
//PageRank.scala
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
 
object PageRank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pagerank")//定义一个sparkConf
    val sc = new SparkContext(conf)//创建Spark的运行环境
    val lines=sc.textFile("hdfs://master:9000/ex3/input");//调用Spark的读文件函数，输出一个RDD类型的实例 具体类型：RDD[String]
 
    val links=lines.map{s=>
      val parts=s.split("\t")
      (parts(0),parts(1).split(","))//<原网页，链接网页链表>
    }.cache()//存在缓存中
    
    var ranks = links.mapValues(v => 1.0)//初始化网页pr值为1.0    
	//原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素
    
    for(i <- 0 until 10){//迭代10次
      val contributions = links.join(ranks).flatMap{//按照key进行内连接
        case(pageId, (links, rank)) => //对于每个网页获得其对应的pr值和链入表
          links.map(link => (link, rank / links.size)) //对链入表的每一个网页计算它的贡献值
      }
	  //flatMap：对集合中每个元素进行操作然后再扁平化。
      
      //<原网页,(链入网页，贡献pr值)>
      ranks = contributions
        .reduceByKey((x,y) => x+y)//将贡献pr值加起来
        .mapValues(v => (0.15 + 0.85*v))//计算新的pr值          
    }
    //按照value递减次序排序保留10位小数且输出为一个文件
    ranks.sortBy(_._2,false).mapValues(v=>v.formatted("%.10f").toString()).coalesce(1,true).saveAsTextFile("hdfs://master:9000/user/bigdata_201900180091/Experiment_3_Spark/output");    
    
    /* _._2等价于t => t._2
	 * map(_._n)表示任意元组tuple对象,后面的数字n表示取第几个数.(n>=1的整数)
     * sortBy
     * 第一个参数是一个函数，该函数的也有一个带T泛型的参数，返回类型和RDD中元素的类型是一致的；
     * 第二个参数是ascending，从字面的意思大家应该可以猜到，是的，这参数决定排序后RDD中的元素是升序还是降序，默认是true，也就是升序；
     * 第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。
     * */
	/*
	 * def coalesce(numPartitions:Int, shuffle:Boolean = false)
	 * 返回一个新的RDD，且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle。
	 * */
  
  }
 
}
```









