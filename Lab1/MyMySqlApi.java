package com.test.maven;

import java.sql.*;
import java.sql.Connection;

public class MyMySqlApi {
	static final String DRIVER="com.mysql.cj.jdbc.Driver";
	static final String DB="jdbc:mysql://localhost/zzyMysql";
	//Database auth
	static final String USER="root";
	static final String PASSWD="123456";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn=null;
		Statement stmt=null;
		try {
			//加载驱动程序
			Class.forName(DRIVER);
			System.out.println("Connecting to a selected database...");
			//打开一个连接
			conn=DriverManager.getConnection(DB, USER, PASSWD);
			//执行一个查询
			stmt=conn.createStatement();
			String sql="insert into Student values('scofield',45,89,100)";
			//stmt.executeUpdate(sql);
			//System.out.println("Inserting records into the table successfully!");
			String sql1="select Name,English from Student where name = 'scofield';";
			ResultSet res = stmt.executeQuery(sql1);
			System.out.println("Name"+"\t\t"+"English");
			while(res.next()) {
				System.out.print(res.getString(1) + "\t\t");
				System.out.println(res.getString(2));
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			if(stmt!=null)
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}


}
