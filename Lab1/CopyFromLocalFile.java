package example;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyFromLocalFile {
	// 判断文件是否存在
	public static boolean test(Configuration conf,String path) {
		try(FileSystem fs = FileSystem.get(conf)){
			return fs.exists(new Path(path));
		}
		catch(IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	// 复制文件到指定路径 若路径已存在，则进行覆盖
	public static void copyFromLocalFile(Configuration conf,String localFilePath,String remoteFilePath) {
		Path localPath = new Path(localFilePath);
		Path remoPath = new Path(remoteFilePath);
		try (FileSystem fs = FileSystem.get(conf)){
			// fs.copyfromlocalfile 第一个参数表示是否要删除元文件，第二个参数表示是否覆盖
			fs.copyFromLocalFile(false, true,localPath,remoPath);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	// 追加文件内容
	public static void appendToFile(Configuration conf,String localFilePath,String remoteFilePath) {
		Path remotePath = new Path(remoteFilePath);
		try (FileSystem fs = FileSystem.get(conf);FileInputStream in = new FileInputStream(localFilePath);){
			FSDataOutputStream out = fs.append(remotePath);
			byte[] data = new byte[1024];
			int read = -1;
			while((read=in.read(data)) > 0) {
				out.write(data,0,read);
			}
			out.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	// main 函数
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		String choice = "append"; // 若文件存在选择追加
		//String choice = "overwrite"; // 若文件存在 选择覆盖
		String localFilePath = "/usr/local/hadoop/text1.txt"; // 本地路径
		String remoteFilePath = "/user/hadoop/text1.txt"; // HDFS路径

		try {
	           /* 判断文件是否存在 */
	           boolean fileExists = false;
	           if (CopyFromLocalFile.test(conf, remoteFilePath)) {
	               fileExists = true;
	               System.out.println(remoteFilePath + " 已存在.");
	           } else {
	               System.out.println(remoteFilePath + " 不存在.");
	           }
	           /* 进行处理 */
	           if (!fileExists) { // 文件不存在，则上传
	               CopyFromLocalFile.copyFromLocalFile(conf, localFilePath,
	                       remoteFilePath);
	               System.out.println(localFilePath + " 已上传至 " + remoteFilePath);
	           } else if (choice.equals("overwrite")) { // 选择覆盖
	               CopyFromLocalFile.copyFromLocalFile(conf, localFilePath,
	                       remoteFilePath);
	               System.out.println(localFilePath + " 已覆盖 " + remoteFilePath);
	           } else if (choice.equals("append")) { // 选择追加
	               CopyFromLocalFile.appendToFile(conf, localFilePath,
	                       remoteFilePath);
	               System.out.println(localFilePath + " 已追加至 " + remoteFilePath);
	           }
	       } catch (Exception e) {
	           e.printStackTrace();
	       }
	

	}

}
