package com.test.maven;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHbaseApi {
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
	     * 查看已有表
	     * @throws IOException
	     */
	    public static void listTables() throws IOException {
	        init();
	        HTableDescriptor hTableDescriptors[] = admin.listTables();
	        for(HTableDescriptor hTableDescriptor :hTableDescriptors){
	            System.out.println(hTableDescriptor.getNameAsString());
	        }
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
	     * 向某一行的某一列插入数据
	     * @param tableName 表名
	     * @param rowKey 行键
	     * @param colFamily 列族名
	     * @param col 列名（如果其列族下没有子列，此参数可为空）
	     * @param val 值
	     * @throws IOException
	     */
	    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
	        init();
	        Table table = connection.getTable(TableName.valueOf(tableName));
	        Put put = new Put(rowKey.getBytes());
	        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
	        table.put(put);
	        table.close();
	        close();
	    } 
	    /**
	     * 删除数据
	     * @param tableName 表名
	     * @param rowKey 行键
	     * @param colFamily 列族名
	     * @param col 列名
	     * @throws IOException
	     */
	    public static void deleteRow(String tableName,String rowKey,String colFamily,String col) throws IOException {
	        init();
	        Table table = connection.getTable(TableName.valueOf(tableName));
	        Delete delete = new Delete(rowKey.getBytes());
	       
	        boolean flag2 =true;
	        while(flag2)
	        {
	        System.out.println("请输入你的选择 1-删除列族的所有数据  2-指定列的数据 ");
	        Scanner scanner=new Scanner(System.in);
	        String chooseString = scanner.nextLine();
	        switch (chooseString) {
	        case "1":
	        {
	            //删除指定列族的所有数据
	            delete.addFamily(colFamily.getBytes());
	            table.delete(delete);
	            table.close();
	            close();
	            break;
	        }
	        case "2":
	        {
	            //删除指定列的数据
	            delete.addColumn(colFamily.getBytes(), col.getBytes());
	            table.delete(delete);
	            table.close();
	            close();
	            break;
	        }
	        

	        default:
	        {
	            System.out.println("   你的输入有误 ！！！    ");
	            table.close();
	            close();
	            break;
	        }
	        }
	        System.out.println(" 你要继续操作吗？ 是-true 否-false ");
	        flag2=scanner.nextBoolean();
	        }
	    }
	    
	    /**
	     * 清空制定的表的所有记录数据
	     * @param args
	     * @throws IOException 
	     */
	    public static void clearRows(String tableName) throws IOException{
	        
	        init();
	        //HBaseAdmin admin1=new HBaseAdmin(configuration);
	        TableDescriptor tDescriptor =admin.getDescriptor(TableName.valueOf(tableName));//读取了之前表的表名 列簇等信息，然后再进行删除操作。 总思想是先将原表结构保留下来，然后进行删除，再重新依据保存的信息重新创建表。
	        TableName tablename=TableName.valueOf(tableName);
	       
	        //删除表
	        admin.disableTable(tablename);
	        admin.deleteTable(tablename);
	        
	        //重新建表
	         admin.createTable(tDescriptor);
	        close();

	    }   
	    public static void countRows (String tableName) throws IOException
	     {
	         init();
	         Table table = connection.getTable(TableName.valueOf(tableName));
	         Scan scan = new Scan();
	         ResultScanner scanner =table.getScanner(scan);
	         int num = 0;
	         for(Result result = scanner.next();result!=null;result=scanner.next())
	         {
	             num++;
	         }
	         System.out.println("行数："+num);
	         scanner.close();
	         close();
	     }
	    
	    public static void main(String[] args)throws IOException {
	        // TODO Auto-generated method stub
	    	MyHbaseApi t =new MyHbaseApi();
	    	boolean flag =true;
	        while(flag)
	        {
		        System.out.println("------------向已经创建好的表中添加和删除指定的列簇或列--------------------");
		        System.out.println("------------请输入您要进行的操作\n"
		        		+ "------------1- 添加	\n"
		        		+ "------------2- 删除	\n"
		        		+ "------------3- 查询所有表\n"
		        		+ "------------4- 查询指定表记录\n"
		        		+ "------------5- 清空指定表的数据\n"
		        		+ "------------6- 统计表的行数\n");
		        Scanner scan = new Scanner(System.in);
		        String choose1=scan.nextLine();
		        switch (choose1) {
			        case "1":
			        {
			            System.out.println("请输入要添加的表名");
			            String tableName=scan.nextLine();
			            System.out.println("请输入要添加的表的行键");
			            String rowKey=scan.nextLine();
			            System.out.println("请输入要添加的表的列簇");
			            String colFamily=scan.nextLine();
			            System.out.println("请输入要添加的表的列名");
			            String col=scan.nextLine();
			            System.out.println("请输入要添加的值");
			            String val=scan.nextLine();
			            try {
			                t.insertRow(tableName, rowKey, colFamily, col, val);
			                System.out.println("插入成功：");
			                t.getData(tableName);
			            } catch (IOException e) {
			                // TODO Auto-generated catch block
			                e.getMessage();
			            }
			            break;
			        }
			        case "2":
			        {
		                System.out.println("请输入要删除的表名");
		                String tableName=scan.nextLine();
		                System.out.println("请输入要删除的表的行键");
		                String rowKey=scan.nextLine();
		                System.out.println("请输入要删除的表的列簇");
		                String colFamily=scan.nextLine();
		                System.out.println("请输入要删除的表的列名");
		                String col=scan.nextLine();
		                try {
		                    System.out.println("----------------------表的原本信息如下---------------------");
		                    t.getData(tableName);
		                    System.out.println("____________________________正在执行删除操作........\n");
		                    t.deleteRow(tableName, rowKey, colFamily, col);
		                    System.out.println("____________________________删除成功_______________\n");
		                    System.out.println("---------------------删除后  表的信息如下---------------------");
		                    t.getData(tableName);
		                } catch (IOException e) {
		                    // TODO Auto-generated catch block
		                    e.getMessage();
		                }
		            break;
		            }
			        case "3":
			        {
			            //列出所有表
			        	System.out.println("以下为Hbase 数据库中所存的表信息");
			            t.listTables();
			            break;
			        }
			        case "4":
			        {
			            //别处制定表的所有数据
			        	System.out.println("请输入要查看的表名");
			            String tableName=scan.nextLine();
			            System.out.println("信息如下：");
			            t.getData(tableName);
			            break;
			        }
			        case "5":
			        {
						System.out.println("请输入要清空的表名");
						String tableName=scan.nextLine();
						System.out.println("表原来的信息：");
						t.getData(tableName);
						t.clearRows(tableName);
						System.out.println("表已清空：");
						break;
			        }
			        case "6":
			        {
			        	System.out.println("请输入要统计行数的表名");
			            String tableName=scan.nextLine();
			            t.countRows(tableName);
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
