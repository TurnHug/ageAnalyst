package Analyst

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import createAge.generateAge
import org.apache.log4j.{Level, Logger}
import java.io.{File, PrintWriter}


object Analyst {

    def ageDistribution(df:DataFrame): Unit ={

        val adf1 = df.filter(df.col("age")> 0 and df.col("age") < 20)
        // 过滤不同年龄段的年龄
        adf1.coalesce(1).write.format("csv").option("header","true")
            .save("hdfs://192.168.75.147:9000/ageAnalyst/ageDistribution1")

        // 统计个数
        val adf2 = df.filter(df.col("age")>= 20 and df.col("age") < 40)

        adf2.coalesce(1).write.format("csv").option("header","true")
            .save("hdfs://192.168.75.147:9000/ageAnalyst/ageDistribution2")


        val adf3 = df.filter(df.col("age")>= 40 and df.col("age") < 60)

        adf3.coalesce(1).write.format("csv").option("header","true")
            .save("hdfs://192.168.75.147:9000/ageAnalyst/ageDistribution3")

        val adf4 = df.filter(df.col("age")>= 60 and df.col("age") < 80)

        adf4.coalesce(1).write.format("csv").option("header","true")
            .save("hdfs://192.168.75.147:9000/ageAnalyst/ageDistribution4")


        val adf5 = df.filter(df.col("age")>= 80 and df.col("age") < 100)

        adf5.coalesce(1).write.format("csv").option("header","true")
            .save("hdfs://192.168.75.147:9000/ageAnalyst/ageDistribution5")


        // 过滤结果保存成文件
        val a1 = adf1.count()
        val a2 = adf2.count()
        val a3 = adf3.count()
        val a4 = adf4.count()
        val a5 = adf5.count()

        // 保存年龄段分布到文件
        val aList = List(a1,a2,a3,a4,a5)

        val li = List("0<age<20:","20<age<40:","40<age<60:","60<age<80:","80<age<100:")

        val f = new File("ageGroup.txt")

        if (!f.exists) f.createNewFile

        val w = new PrintWriter("ageGroup.txt")

        w.write("不同年龄段的人数："+"\n")
        for (i <- 0 until 4) {
            w.write(li.apply(i))
            w.write(aList.apply(i)+"\n")
        }
        w.close()

        generateAge.unloadHdfs("ageGroup.txt","hdfs://192.168.75.147:9000/ageAnalyst")

        println("年龄分布已保存在ageAnalyst文件中!")
    }

    def countAge(list: List[String]):Unit={
        val cou = list.flatMap(x => x.split(",") //1.转化为List扁平化		1.切割
            .filter(x => x.trim.nonEmpty)) //2.过滤空字符及前后空格		 2.分组
            .groupBy(x => x) //3.一个个分组				3.排序
            .mapValues(_.size) //4.取map的值
            .toList //5.转换成List
            .sortBy(-_._2) //6.按次数排序 降序

        val file = new File("countAge.txt")

        if (!file.exists)
            file.createNewFile

        val wr = new PrintWriter("countAge.txt")

        for (i <- cou) {
            wr.write(i.toString())

        }
        wr.close()

        generateAge.unloadHdfs("countAge.txt","hdfs://192.168.75.147:9000/ageAnalyst")

        println("年龄统计已保存在ageAnalyst文件中！")

    }

    def main(args: Array[String]): Unit = {

        generateAge.main()

        val sc = new SparkConf().setMaster("spark://192.168.75.147:7077").setAppName("ageAnalyst")
            .set("spark.jars","/D:/lsx/ageAnalyst/out/artifacts/ageAnalyst_jar/ageAnalyst.jar")

        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

        Logger.getLogger("org").setLevel(Level.ERROR)
//        //创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(sc).getOrCreate()


        val df: DataFrame = spark.read
            .option("header", "false") //默认为false，表示第一行不是表头
            .option("sep", ",") //列分隔符
            .csv("hdfs://192.168.75.147:9000/ageAnalyst/age.txt") //读取HDFS文件
            .withColumnRenamed("_c0", "id") //给列起别名
            .withColumnRenamed("_c1", "age")

        val ageSum = df.agg("age"->"sum")

        val s = ageSum.take(1).mkString.filter(x=>x !='['&& x!= ']').toFloat

        val count = df.count().toInt

        val average = s/count

        println("共生成了"+count+"组年龄数据,平均年龄"+average+"岁！")

        ageDistribution(df)

        val rdd= df.select("age").collect().map(_(0)).toList.map(x=>x.toString)
        // df转list

        countAge(rdd)

        spark.stop()

    }
}
