package example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;

public class CopyToLocal {
	//copy文件到本地 判断本地路径是否已存在，若已存在，则自动进行重命名
	 public static void copyToLocal(Configuration conf, String remoteFilePath,
	            String localFilePath) {
	        Path remotePath = new Path(remoteFilePath);
	        try (FileSystem fs = FileSystem.get(conf)) {
	            File f = new File(localFilePath);
	            /* 如果文件名存在，自动重命名(在文件名后面加上 _0, _1 ...) */
	            if (f.exists()) {
	                System.out.println(localFilePath + " 已存在.");
	                Integer i = Integer.valueOf(0);
	                
	                // 提取.txt 前面的部分进行重命名
	                String temp_LocalFilePath = localFilePath.split("\\.")[0];
	                //System.out.println(temp_LocalFilePath);
	                
	                // 使用while 保证一定能成功重命名
	                while (true) {
	                    f = new File(temp_LocalFilePath + "_" + i.toString() + ".txt");
	                    if (!f.exists()) {
	                        localFilePath = temp_LocalFilePath + "_" + i.toString() + ".txt";
	                        break;
	                    } else {
	                        i++;
	                        continue;
	                    }
	                }
	                System.out.println("将重新命名为: " + localFilePath);
	            }
	            // 下载文件到本地
	            Path localPath = new Path(localFilePath);
	            fs.copyToLocalFile(remotePath, localPath);
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	    }
	 // main汉书
	 public static void main(String[] args) {
	        Configuration conf = new Configuration();
	        conf.set("fs.defaultFS", "hdfs://localhost:9000");
	        String localFilePath = "/usr/local/hadoop/text1.txt"; // 本地路径
	        String remoteFilePath = "/user/hadoop/text1.txt"; // HDFS路径
	        try {
	            CopyToLocal.copyToLocal(conf, remoteFilePath, localFilePath);
	            System.out.println(remoteFilePath+" 下载完成");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }

}
