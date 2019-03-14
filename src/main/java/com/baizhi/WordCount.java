package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Title: com.baizhi.WordCount
 * @ProjectName MapReduce
 * @Description: TODO
 * @author 谢浩哲
 * @date 2019/3/1419:13
 */

public class WordCount {
    /**
     * 创建静态内部类，执行Map（映射）任务 ,需要继承Mapper类 并重写map方法
     * keyIn  输入key的类型 LongWritable  每行数据首字母偏移量的值
     * valueIn 输入value的类型 Text 一行文本数据 Text 类型相当于String类型
     * keyOut Text 输出key的类型
     * valueOut 输出value的类型 IntWritable
     */
    public static class MyMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        /**
         * 重写Mapper类中的map方法
         * @param key 每行数据首字母偏移量 keyIn
         * @param value 每行文本数据 valueIn
         * @param context 上下文对象
         * @throws IOException IO异常
         * @throws InterruptedException 类型转换异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //每个单词之间使用空格符隔开，根据空格符将每行数据拆分为单个单词，用于统计
            // Welcome to Beijing
            //[ Welcome,to,Beijing]
            String[] s = value.toString().split(" ");
            for (String s1 : s) {
                //每个单词出现一次就记录为1
                //以 Welcome 1 的格式写出去
                context.write(new Text(s1), new IntWritable(1));
            }
        }
    }

    /**
     * 创建静态内部类 编写Reduce 应用程序 需要继承Reducer类 并重 reduce方法
     * keyIn map应用程序中的keyOut Text 类型
     * valueIn map应用程序中的valueOut IntWritable 类型
     * keyOut 计算结果输出的key类型 Text
     * valueOut 计算结果输出的value类型 IntWritable
     */
    public static class MyRecuder extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * 重写Reducer类中的reduce方法
         *
         * @param key     map应用程序输出的key
         * @param values  map应用程序输出的value
         * @param context 上下文对象
         * @throws IOException          IO异常
         * @throws InterruptedException 类型转换异常
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //定义一个变量用来计算数量的和
            int total = 0;
            for (IntWritable value : values) {
                //获取value的值 并求和
                total+=value.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    /**
     * 初始化MapReducer程序
     * @param args 可变长参数
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //初始化MapReducer对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "word count");
        //设置MapReducer程序的启动入口
        job.setJarByClass(WordCount.class);
        //设置数据的输入类型和输出类型
        //inputFormat 决定如何切割数据集 如何读取切割后的数据
        //outputFormat 决定如何输出计算结果
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据的来源和计算结果的输出目的地
//        TextInputFormat.addInputPath(job, new Path(args[0]));
//        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        TextInputFormat.addInputPath(job, new Path("C:\\Users\\24309\\Desktop\\test.txt"));
        TextOutputFormat.setOutputPath(job,new Path("D:\\result"));
        //设置keyOut和valueOut的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置初始化MapReducer程序的Map任务的实现类和Reduce任务的实现类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyRecuder.class);
        //提交MapReduce程序
        job.waitForCompletion(true);
    }
}
