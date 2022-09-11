package createAge;

import java.io.*;
import org.apache.hadoop.fs.Path;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


public class generateAge {

    public static void getAge(){  // 生成年龄文件
        try {
            File f = new File("age.txt");

            if (!f.exists()) {
                f.createNewFile();
            }
            Random r = new Random();
            PrintWriter w = new PrintWriter("age.txt");
            for (int i = 1; i < 100000; i++) {

                w.write(i+","+r.nextInt(99)+"\n");

            }
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void unloadHdfs(String localDir, String hdfsDir){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",hdfsDir);
        try{
            Path localPath = new Path(localDir);
            Path hdfsPath = new Path(hdfsDir);
            FileSystem hdfs = FileSystem.get(conf);
            if(!hdfs.exists(hdfsPath)){
                hdfs.mkdirs(hdfsPath);
            }
            hdfs.copyFromLocalFile(localPath, hdfsPath);
        }catch(Exception e){
            e.printStackTrace();
        }
    }



    public static void main( ) {
        String localDir = "age.txt";
        String hdfsDir = "hdfs://192.168.75.147:9000/ageAnalyst";
        getAge();
        unloadHdfs(localDir,hdfsDir);

    }
}
