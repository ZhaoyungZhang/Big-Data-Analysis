package com.test.maven;

import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class MyHbaseApi2 {
	public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    
    //建立连接
    public static void init(){
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    
    /**
     * 建表。参数tableName为表的名称，字符串数组fields为存储记录各个域名称的数组。
     * 要求当HBase已经存在名为tableName的表时，先删除原有的表，然后再
     * 创建新的表  field：列族
     * @param myTableName 表名
     * @param colFamily 列族名
     * @throws IOException
     */
    public static void createTable(String tableName,String[] fields) throws IOException {	 
        init();
        TableName tablename = TableName.valueOf(tableName);
        if(admin.tableExists(tablename)){
            System.out.println("表已存在，将执行删除原表，重建新表!");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);//删除原来的表
        }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String str:fields){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("表已创建成功");	    
        close();
    }
    
    /**
     * 向表 tableName、行 row（用 S_Name 表示）和字符串数组 fields 指定的单元格中
     * 添加对应的数据 values。
     * 其中，fields 中每个元素如果对应的列族下还有相应的列限定符的话，
     * 用“columnFamily:column”表示。
     * 例如，同时向“Math”、“Computer Science”、“English”三列添加成绩时，
     * 字符串数组 fields 为{“Score:Math”, ”Score:Computer Science”, ”Score:English”}，
     * 数组values 存储这三门课的成绩。
     */
    public static void addRecord(String tableName,String rowKey,String []fields,String [] values) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
        	Put put = new Put(rowKey.getBytes());
        	String [] cols = fields[i].split(":");
        	if(cols.length==1)
        	{
        		put.addColumn(cols[0].getBytes(), "".getBytes(), values[i].getBytes());//因为当输入的是单列族，split仅读出一个字符字符串，即cols仅有一个元素
        	}
        	else {
        		put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
        	}
        	table.put(put);
         }
        table.close();
        close();
    }
    /**
     * 根据表名查找表信息
     */
    public static void getData(String tableName)throws  IOException{
    	init();
	    Table table = connection.getTable(TableName.valueOf(tableName));
	    Scan scan = new Scan();
	    ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner)
        {
        	showCell((result));
        }
        close();
    }
    /**
     * 格式化输出
     * @param result
     */
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName(行键):"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp(时间戳):"+cell.getTimestamp()+" ");
            System.out.println("column Family（列簇）:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("column Name（列名）:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:（值）"+new String(CellUtil.cloneValue(cell))+" ");
            System.out.println();
        }
    }
    
    /**
     * 浏览表 tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回 null。
     * 要求当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；
     * 当参数 column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
     * @param tableName
     * @param column
     * @throws IOException
     */

    public static void scanColumn (String tableName,String column) throws IOException
    {
    	init();
    	Table table = connection.getTable(TableName.valueOf(tableName));
    	Scan scan = new Scan();
    	String [] cols = column.split(":");
    	if(cols.length==1)
    	{
    		scan.addFamily(Bytes.toBytes(column));
    	}
    	else {
    		scan.addColumn(Bytes.toBytes(cols[0]),Bytes.toBytes(cols[1]));
    	}
        ResultScanner scanner = table.getScanner(scan);
        boolean data_exist = false;
        for (Result result = scanner.next(); result !=null;result = scanner.next()) {
        	showCell(result);
        	data_exist = true;
        }
        if(!data_exist) {
        	System.out.println("null");
        }
       table.close();
       close();
    }
    
    /**
     * 修改表 tableName，行 row（可以用学生姓名 S_Name 表示），列 column 指定的单元格的数据。
     * @throws IOException
     */
    public static void modifyData(String tableName,String rowKey,String column,String value) throws IOException
    {
    	init();
    	Table table = connection.getTable(TableName.valueOf(tableName));
    	Put put = new Put(rowKey.getBytes());
    	String [] cols = column.split(":");
    	if(cols.length==1)
    	{
    		put.addColumn(column.getBytes(),"".getBytes() , value.getBytes());//qualifier:列族下的列名
    	}
    	else {
    		put.addColumn(cols[0].getBytes(),cols[1].getBytes() , value.getBytes());//qualifier:列族下的列名
    	}
    	table.put(put);
    	table.close();
    	close();
    }
    
    /**
     * 删除表 tableName 中 row 指定的行的记录。
     * @throws IOException
     */
    public static void deleteRow(String tableName,String rowKey) throws IOException
    {
    	init();
    	Table table = connection.getTable(TableName.valueOf(tableName));
    	Delete delete = new Delete(rowKey.getBytes());  
    	table.delete(delete);
    	table.close();
    	close();
    }

    public static void main(String[] args) throws IOException {
    	MyHbaseApi2 test_Two = new MyHbaseApi2();
    	boolean flag =true;
    	while(flag)
    	{
    		System.out.println("------------------------------------------------提供以下功能----------------------------------------------");
	    	System.out.println("                       1- createTable  创建表,覆盖已有                                      ");
	    	System.out.println("                       2- addRecord    向已知表名、行键、列簇的表添加值                       ");
	    	System.out.println("                       3- ScanColumn   浏览表某一列的数据                                           ");
	    	System.out.println("                       4- modifyData   修改某表某行某列，指定单元格的数据    ");
	    	System.out.println("                       5- deleteRow    删除某表某行的记录                                                 ");
	    	System.out.println("--------------------------------------------------------------------------------------------------------");
	    	Scanner scan = new Scanner(System.in);
	    	String choose1=scan.nextLine();
	    	switch (choose1) {
	    	case "1":
	    	{
		    	System.out.println("请输入要创建的表名");
		    	String tableName=scan.nextLine();
		    	System.out.println("请输入要创建的表的列族个数");
		    	int Num=scan.nextInt();
		    	String [] fields = new String[Num];
		    	System.out.println("请输入要创建的表的列族");
		    	/* Scanner scanner = new Scanner(System.in);     scanner.next 如不是全局，即会记得上一次输出。相同地址读入值时*/
		    	for(int i=0;i< fields.length;i++)
		    	{
			    	/*BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			    	fields[i] = in.readLine();*/
			    	/*fields[i]=scan.next(); 因为之前没有输入过，所以可以读入新值*/
			    	scan = new Scanner(System.in);
			    	fields[i]=scan.nextLine();
		    	}
		    	System.out.println("正在执行创建表的操作");
    	        test_Two.createTable(tableName,fields);
    	        break;
	    	}
	    	case "2":
	    	{
		    	System.out.println("请输入要添加数据的表名");
		    	String tableName=scan.nextLine();
		    	System.out.println("请输入要添加数据的表的行键");
		    	String rowKey=scan.nextLine();
		    	System.out.println("请输入要添加数据的表的列的个数");
		    	int num =scan.nextInt();
		    	String fields[]=new String[num];
		    	System.out.println("请输入要添加数据的表的列信息 共"+num+"条信息");
		    	for(int i=0;i< fields.length;i++)
		    	{
		    		BufferedReader in3= new BufferedReader(new InputStreamReader(System.in));
		    		fields[i] = in3.readLine();
		    		/*fields[i]=scan.next(); 因为之前没有输入过，所以可以读入新值*/
		    	}
		    	System.out.println("请输入要添加的数据信息 共"+num+"条信息");
		    	String values[]=new String[num];
		    	for(int i=0;i< values.length;i++)
		    	{
		    		BufferedReader in2 = new BufferedReader(new InputStreamReader(System.in));
		    		values[i] = in2.readLine();
		    	}
		    	System.out.println("原表信息");
		    	test_Two.getData(tableName);
		    	System.out.println("正在执行向表中添加数据的操作........\n");
		    	test_Two.addRecord(tableName, rowKey, fields, values);
		    	System.out.println("\n添加后的表的信息........");
		    	test_Two.getData(tableName);
		    	break;
	    	}
	    	case "3":
	    	{
		    	System.out.println("请输入要查看数据的表名");
		    	String tableName=scan.nextLine();
		    	System.out.println("请输入要查看数据的列名");
		    	String column=scan.nextLine();	
		    	System.out.println("查看的信息如下：........\n");	
		    	test_Two.scanColumn(tableName, column);
		    	break;	
	    	}	
	    	case "4":
	    	{
	    		System.out.println("请输入要修改数据的表名");
	    		String tableName=scan.nextLine();
		    	System.out.println("请输入要修改数据的表的行键");
		    	String rowKey=scan.nextLine();
		    	System.out.println("请输入要修改数据的列名");
		    	String column=scan.nextLine();
		    	System.out.println("请输入要修改的数据信息  ");
		    	String value=scan.nextLine();
		    	System.out.println("原表信息如下：........\n");
		    	test_Two.getData(tableName);
		    	System.out.println("正在执行向表中修改数据的操作........\n");
		    	test_Two.modifyData(tableName, rowKey, column, value);
		    	System.out.println("\n修改后的信息如下：........\n");
		    	test_Two.getData(tableName);
		    	break;
	    	}
	    	case "5":
	    	{
		    	System.out.println("请输入要删除指定行的表名");
		    	String tableName=scan.nextLine();
		    	System.out.println("请输入要删除指定行的行键");
		    	String rowKey=scan.nextLine();
		    	System.out.println("原表信息如下：........\n");	
		    	test_Two.getData(tableName);	
		    	System.out.println("正在执行向表中删除数据的操作........\n");	
		    	test_Two.deleteRow(tableName, rowKey);	
		    	System.out.println("\n删除后的信息如下：........\n");	
		    	test_Two.getData(tableName);	
		    	break;	
	    	}
	    	default:
	    	{
		    	System.out.println("   你的操作有误 ！！！    ");
		    	break;
	    	}
    	}
    	System.out.println(" 你要继续操作吗？ 是-true 否-false ");
    	flag=scan.nextBoolean();
    	}
    System.out.println("   程序已退出！    ");
    }    
}
