package com.test.maven;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.UnknownHostException;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.BasicDBObject;
import com.mongodb.*;

public class MyMongoApiTest {
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		//mongo客户端
		MongoClient  mongoClient= new MongoClient("localhost",27017);
		//mongo数据库
		DB mongoDatabase = mongoClient.getDB("zzyMysql");
		//数据库中某个集合
		DBCollection collection = mongoDatabase.getCollection("Student");
		
		// 插入
		//实例化文档
		BasicDBObject document=new BasicDBObject("name","scofield").append("score", new BasicDBObject("English",45).append("Math", 89).append("Computer", 100));
        //将文档插入
        collection.insert(document);  
        System.out.println("scofield文档插入成功"); 
        
        // find
        BasicDBObject query = new BasicDBObject().append("name", "scofield");
		BasicDBObject hidden = new BasicDBObject().append("score",1).append("_id", 0);
		DBCursor cursor = collection.find(query,hidden);
		 while (cursor.hasNext()) {
			 System.out.println(cursor.next());
		 }
	}
}
