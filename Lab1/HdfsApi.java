package example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;

public class HdfsApi {
	 /**
	  * 判断路径是否存在
	  */
	 public static boolean test(Configuration conf, String path) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     return fs.exists(new Path(path));
	 }
	 /**
	  * 创建目录
	  */
	 public static boolean mkdir(Configuration conf, String remoteDir) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path dirPath = new Path(remoteDir);
	     boolean result = fs.mkdirs(dirPath);
	     fs.close();
	     return result;
	 }
	 /**
	  * 创建文件
	  */
	 public static void touchz(Configuration conf, String remoteFilePath) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path remotePath = new Path(remoteFilePath);
	     FSDataOutputStream outputStream = fs.create(remotePath);
	     outputStream.close();
	     fs.close();
	 }	 
	 
	 /**
	  * 追加文本内容
	  */
	 public static void appendContentToFile(Configuration conf, String content, String remoteFilePath) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path remotePath = new Path(remoteFilePath);
	     /* 创建一个文件输出流，输出的内容将追加到文件末尾 */
	     FSDataOutputStream out = fs.append(remotePath);
	     out.write(content.getBytes());
	     out.close();
	     fs.close();
	}

	 /**
	  * 追加文件内容
	  */
	 public static void appendToFile(Configuration conf, String localFilePath, String remoteFilePath) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path remotePath = new Path(remoteFilePath);
	     /* 创建一个文件读入流 */
	     FileInputStream in = new FileInputStream(localFilePath);
	     /* 创建一个文件输出流，输出的内容将追加到文件末尾 */
	     FSDataOutputStream out = fs.append(remotePath);
	     /* 读写文件内容 */
	     byte[] data = new byte[1024];
	     int read = -1;
	     while ( (read = in.read(data)) > 0 ) {
	         out.write(data, 0, read);
	     }
	     out.close();
	     in.close();
	     fs.close();
	 }

	 /**
	  * 移动文件到本地
	  * 移动后，删除源文件
	  */
	 public static void moveToLocalFile(Configuration conf, String remoteFilePath, String localFilePath) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path remotePath = new Path(remoteFilePath);
	     Path localPath = new Path(localFilePath);
	     fs.moveToLocalFile(remotePath, localPath);
	 }

	 
	 /**
	  * 判断目录是否为空
	  * true: 空，false: 非空
	  */
	 public static boolean isDirEmpty(Configuration conf, String remoteDir) throws IOException {
	     FileSystem fs = FileSystem.get(conf);
	     Path dirPath = new Path(remoteDir);
	     RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(dirPath, true);
	     return !remoteIterator.hasNext();
	 }

	 /**
	  * 删除目录
	   */
	  public static boolean rmDir(Configuration conf, String remoteDir) throws IOException {
	      FileSystem fs = FileSystem.get(conf);
	      Path dirPath = new Path(remoteDir);
	      /* 第二个参数表示是否递归删除所有文件 */
	      boolean result = fs.delete(dirPath, true);
	      fs.close();
	      return result;
	  }

	  /**
	   * 删除文件
	   */
	  public static boolean rm(Configuration conf, String remoteFilePath) throws IOException {
	      FileSystem fs = FileSystem.get(conf);
	      Path remotePath = new Path(remoteFilePath);
	      boolean result = fs.delete(remotePath, false);
	      fs.close();
	      return result;
	  }

	  /**
	   * 移动文件
	   */
	  public static boolean mv(Configuration conf, String remoteFilePath, String remoteToFilePath) throws IOException {
	      FileSystem fs = FileSystem.get(conf);
	      Path srcPath = new Path(remoteFilePath);
	      Path dstPath = new Path(remoteToFilePath);
	      boolean result = fs.rename(srcPath, dstPath);
	      fs.close();
	      return result;
	  }

	  
	 /**
	  * 主函数 6
	  */
//	 public static void main(String[] args) {
//	     Configuration conf = new Configuration();
//	 conf.set("fs.default.name","hdfs://localhost:9000");
//	     String remoteFilePath = "/user/hadoop/test/text3.txt";    // HDFS路径
//	     String remoteDir = "/user/hadoop/text1_0.txt";    // HDFS路径对应的目录
//
//	     try {
//	         /* 判断路径是否存在，存在则删除，否则进行创建 */
//	         if ( HdfsApi.test(conf, remoteFilePath) ) {
//	             HdfsApi.rm(conf, remoteFilePath); // 删除
//	             System.out.println("删除文件: " + remoteFilePath);
//	         } else {
//	             if ( !HdfsApi.test(conf, remoteDir) ) { // 若目录不存在，则进行创建
//	                 HdfsApi.mkdir(conf, remoteDir);
//	                 System.out.println("创建文件夹: " + remoteDir);
//	             }
//	             HdfsApi.touchz(conf, remoteFilePath);
//	             System.out.println("创建文件: " + remoteFilePath);
//	         }
//	     } catch (Exception e) {
//	         e.printStackTrace();
//	     }
//	 }
	 
	 
	  /**
	   * 主函数7
	   */
//	  public static void main(String[] args) {
//	      Configuration conf = new Configuration();
//	  conf.set("fs.default.name","hdfs://localhost:9000");
//	      String remoteDir = "/user/hadoop/test";    // HDFS目录
//	      Boolean forceDelete = false;  // 是否强制删除
//
//	      try {
//	          /* 判断目录是否存在，不存在则创建，存在则删除 */
//	          if ( !HdfsApi.test(conf, remoteDir) ) {
//	              HdfsApi.mkdir(conf, remoteDir); // 创建目录
//	              System.out.println("创建目录: " + remoteDir);
//	          } else {
//	              if ( HdfsApi.isDirEmpty(conf, remoteDir) || forceDelete ) { // 目录为空或强制删除
//	                  HdfsApi.rmDir(conf, remoteDir);
//	                  System.out.println("删除目录: " + remoteDir);
//	              } else  { // 目录不为空
//	                  System.out.println("目录不为空，不删除: " + remoteDir);
//	              }
//	          }
//	      } catch (Exception e) {
//	          e.printStackTrace();
//	      }
//	  }

	  
	 /**
	  * 主函数8
	  */
//	 public static void main(String[] args) {
//	     Configuration conf = new Configuration();
//	 conf.set("fs.default.name","hdfs://localhost:9000");
//	     String remoteFilePath = "/user/hadoop/text1.txt";    // HDFS文件
//	     String content = "新追加的内容\n";
//	     //String choice = "after";         //追加到文件末尾
//         String choice = "before";    // 追加到文件开头
//
//	     try {
//	         /* 判断文件是否存在 */
//	         if ( !HdfsApi.test(conf, remoteFilePath) ) {
//	             System.out.println("文件不存在: " + remoteFilePath);
//	         } else {
//	             if ( choice.equals("after") ) { // 追加在文件末尾
//	                 HdfsApi.appendContentToFile(conf, content+choice, remoteFilePath);
//	                 System.out.println("已追加内容到文件末尾" + remoteFilePath);
//	             } else if ( choice.equals("before") )  { // 追加到文件开头
//	                 /* 没有相应的api可以直接操作，因此先把文件移动到本地，创建一个新的HDFS，再按顺序追加内容 */
//	                 String localTmpPath = "/usr/local/hadoop/tmp.txt";
//	                 HdfsApi.moveToLocalFile(conf, remoteFilePath, localTmpPath);  // 移动到本地
//	                 HdfsApi.touchz(conf, remoteFilePath);    // 创建一个新文件
//	                 HdfsApi.appendContentToFile(conf, content+choice, remoteFilePath);   // 先写入新内容
//	                 HdfsApi.appendToFile(conf, localTmpPath, remoteFilePath);   // 再写入原来内容
//	                 System.out.println("已追加内容到文件开头: " + remoteFilePath);
//	             }
//	         }
//	     } catch (Exception e) {
//	         e.printStackTrace();
//	     }
//	 }
	  
	  /**
	   * 主函数9
	   * */
//	  public static void main(String[] args) {
//		     Configuration conf = new Configuration();
//		 conf.set("fs.default.name","hdfs://localhost:9000");
//		     String remoteFilePath = "/user/hadoop/text2.txt";    // HDFS文件
//
//		     try {
//		         if ( HdfsApi.rm(conf, remoteFilePath) ) {
//		             System.out.println("文件删除: " + remoteFilePath);
//		         } else {
//		             System.out.println("操作失败（文件不存在或删除失败）");
//		         }
//		     } catch (Exception e) {
//		         e.printStackTrace();
//		     }
//	  }
	  
	  /**
	   * 主函数10
	   * */
	  public static void main(String[] args) {
		     Configuration conf = new Configuration();
		 conf.set("fs.default.name","hdfs://localhost:9000");
		     String remoteFilePath = "hdfs:///user/hadoop/test/text1.txt";    // 源文件HDFS路径
		     String remoteToFilePath = "hdfs:///user/hadoop";    // 目的HDFS路径

		     try {
		         if ( HdfsApi.mv(conf, remoteFilePath, remoteToFilePath) ) {
		             System.out.println("将文件 " + remoteFilePath + " 移动到 " + remoteToFilePath);
		         } else {
		                 System.out.println("操作失败(源文件不存在或移动失败)");
		         }
		     } catch (Exception e) {
		         e.printStackTrace();
		     }
		 }

}
