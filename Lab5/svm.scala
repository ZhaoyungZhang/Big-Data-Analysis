import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import java.util.Properties
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val train_data = sc.textFile("/dbtaobao/dataset/train_after.csv")
val test_data = sc.textFile("/dbtaobao/dataset/test_after.csv")
val train= train_data.map{line =>
  val parts = line.split(',')
  LabeledPoint(parts(4).toDouble,Vectors.dense(parts(1).toDouble,parts
(2).toDouble,parts(3).toDouble))
}
val test = test_data.map{line =>
  val parts = line.split(',')
  LabeledPoint(parts(4).toDouble,Vectors.dense(parts(1).toDouble,parts(2).toDouble,parts(3).toDouble))
}
val numIterations = 1000
val model = SVMWithSGD.train(train, numIterations)
model.clearThreshold()
val scoreAndLabels = test.map{point =>
  val score = model.predict(point.features)
  score+" "+point.label
}
scoreAndLabels.foreach(println)
model.setThreshold(0.0)
scoreAndLabels.foreach(println)
model.clearThreshold()
val scoreAndLabels = test.map{point =>
  val score = model.predict(point.features)
  score+" "+point.label
}
//设置回头客数据
val rebuyRDD = scoreAndLabels.map(_.split(" "))
/下面要设置模式信息
val schema = StructType(List(StructField("score", StringType, true),StructField("label", StringType, true)))
//下面创建Row对象，每个Row对象都是rowRDD中的一行
val rowRDD = rebuyRDD.map(p => Row(p(0).trim, p(1).trim))
//建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
val rebuyDF = spark.createDataFrame(rowRDD, schema)
//下面创建一个prop变量用来保存JDBC连接参数
val prop = new Properties()
prop.put("user", "root") //表示用户名是root
prop.put("password", "root") //表示密码是hadoop
prop.put("driver","com.mysql.jdbc.Driver") //表示驱动程序是com.mysql.jdbc.Driver
//下面就可以连接数据库，采用append模式，表示追加记录到数据库dbtaobao的rebuy表中
rebuyDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/dbtaobao", "dbtaobao.rebuy", prop)

// 部分sql 和 命令
// cd /usr/local/spark
// ./bin/spark-shell --jars /usr/local/spark/jars/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar --driver-class-path /usr/local/spark/jars/mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar


// UPDATE rebuy SET label='0.0' WHERE score < '90000';

// select * from (select count(*) pos from rebuy where label='0.0') tt join (select count(*) neg from rebuy where  label='1.0') ttt join (select count(*) num from rebuy) tttt
// select * from (select count(*) score from rebuy where score='1.0') tt join (select count(*) lable from rebuy where  label='1.0') ttt join (select count(*) num from rebuy) tttt