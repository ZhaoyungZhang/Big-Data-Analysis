package com.test.maven;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

public class MyHbaseApiSql {
	public static Configuration configuration;  // 管理HBase的配置信息
	public static Connection connection;        // 管理HBase的连接
	public static Admin admin;                  // 管理Hbase数据库的表信息
	
	
	public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
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

	

	public static void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException{
		// 获取表
		Table table = connection.getTable(TableName.valueOf(tableName));
		// get rowkey
		Get get = new Get(rowKey.getBytes());
		// get column
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);//从指定的行的某些单元格中取出相应的值
        showCell(result);
        table.close();
    }
	  public static void showCell(Result result){//显示结果信息函数
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("Column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("Row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("Value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }


	
	public static void main(String[] args) {
		configuration = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection();
			admin = connection.getAdmin();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		try {
//			insertRow("Student","Scofield","Score","English","45");
//			insertRow("Student","Scofield","Score","Math","89");
//			insertRow("Student","Scofield","Score","Computer","100");
//			System.out.println("Inserting Data Succeed!");
			getData("Student","Scofield","Score","English");
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		close();
	}
}
