// PageRank.scala
// 导入Conf 以及 上下文
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object PageRank {
    def main(args: Array[String]): Unit = {
        //定义一个sparkConf 设置Name
        val conf = new SparkConf().setAppName("Pagerank")
        // 创建Spark上下文
        val sc = new SparkContext(conf)
        // 调用Spark的读文件函数，输出一个RDD类型的实例lines  具体类型：RDD[String]
        val lines=sc.textFile("hdfs://master:9000/ex3/input");
        // 将 <原网页，链接网页链表> --> 
        val links=lines.map{s=>
            val parts=s.split("\t")
            (parts(0),parts(1).split(","))
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
  }
}