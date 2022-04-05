## 源代码说明





### 201900180091.java

201900180091(1).java 是 mapreduce部分实验1的java程序

201900180091(2).java 是 mapreduce部分实验2的java程序

201900180091(3).java 是 mapreduce部分实验3的java程序

---

### CopyFromLocalFile.java

​	实现从本地文件系统，向HDFS上传文本文件，若HDFS已经存在该文件，可以选择追加或者覆盖。

### CopyToLocal.java

​	实现从HDFS下载指定文件，如果本地已经存在同名文件，则自动对其重命名。

---

### OutputFile.txt

#### cat函数

实现HDFS的指定文件的内容的输出

#### ls函数

实现HDFS 中指定的文件的读写权限、大小、创建时间、路径等信息

#### rls函数

给定HDFS 中某一个目录，递归输出该目录下的所有文件的读写权限、大小、创建时间、路径等信息

### HdfsApi.java

#### test函数

判断路径是否存在

#### mkdir函数

创建目录

#### touchz函数

创建文件

#### appendContentToFile函数

追加文本内容到文本末尾

#### appendToFile函数

追加到文本前面

#### moveToLocalFile函数

移动文件到本地后，读取后删除源文件

#### isDirEmpty函数

判断目录是否为空

#### rmDir函数

删除目录，也能够递归删除所有文件

#### rm函数

 删除文件

#### mv函数

移动文件

---

### MyHbaseApi.java

实现了

#### ListTables

使用HBaseAdmin 列出HBase 所有的表的相关信息，例如表名；

#### getData

根据表明查找表的信息，在终端打印出指定的表的所有记录数据；

#### showCell

格式化输出表

#### insertRow

向已经创建好的表添加指定的列族或列；

#### deleteRow

向已经创建好的表删除指定的列族或列；

#### clearRows

清空指定的表的所有记录数据；

#### countRows

统计表的行数

---

### MyHbaseApi2.java

#### createTable(String tableName, String[] fields)

创建表，当HBase 已经存在名为tableName 的表的时候，先删除原有的表，然后再创建新的表。

#### addRecord(String tableName, String row, String[] fields, String[] values)

向表tableName、行row（用S_Name 表示）和字符串数组fields 指定的单元格中添加对应
的数据values 。其中， fields 中每个元素如果对应的列族下还有相应的列限定符的话， 用
“columnFamily:column”表示。例如，同时向“Math”、“Computer Science”、“English”三列添加成绩时，
字符串数组fields 为{“Score:Math”, ”Score:Computer Science”, ”Score:English”}，数组values 存储这
三门课的成绩。

#### scanColumn(String tableName, String column)

浏览表tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回null。要求
当参数column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列
的数据；当参数column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。

#### modifyData(String tableName, String row, String column)

修改表tableName，行row（可以用学生姓名S_Name 表示），列column 指定的单元格的
数据。

#### deleteRow(String tableName, String row)

删除表tableName 中row 指定的行的记录。

---

### MyMySqlApi.java

实现了mysql的创建表、查询、修改、增添

### MyHbaseApiSql.java

实现了Hbase的创建表、查询、修改、增添

### MyRedisApi.java

实现了Redis的创建表、查询、修改、增添

### MyMongoApi.java

实现了Mongo的创建表、查询、修改、增添

---

### MapReduceTest.java

实现文件的合并和去重

### MapReduceMergeSort.java

实现对输入文件的排序

### InformationMiningTest.java

对指定的表格进行信息挖掘(Join操作)



