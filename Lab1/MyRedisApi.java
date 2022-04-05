package com.test.maven;
import java.util.Map;
import redis.clients.jedis.Jedis;

public class MyRedisApi {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Jedis jedis = new Jedis("localhost");
		//插入scofield的成绩信息
		jedis.hset("student.scofield", "English","45");
		jedis.hset("student.scofield", "Math","89");
		jedis.hset("student.scofield", "Computer","100");
		System.out.println("Inserting values succeed!");
		Map<String,String>  value = jedis.hgetAll("student.scofield");
		for(Map.Entry<String, String> entry:value.entrySet())
		{
         	//输出scofield的成绩信息
			System.out.println("scofield's Score is as follows:");
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
	}

}
