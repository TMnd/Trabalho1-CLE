//package com.trabalho1.urlfrequency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Urlfrequency {

    public static final Log log = LogFactory.getLog(UrlMapper.class);

    public static class UrlMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line);

            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class UrlReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
    

    public static class UrlMapper2 extends Mapper<Object, Text, Text, Text> {
        private Text chave = new Text();
        private Text word = new Text();
        int aux = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String auxKey= "" + aux;
            word.set(line);
            chave.set(auxKey);
            context.write(chave, word);
        }
    }

    public static class UrlReduce2 extends Reducer<Text, Text, Text, Text> {
        private Text chave = new Text();
        private Text result = new Text();
       
        private int total(ArrayList<String> s1) {   
            int total = 0;
        
            for(String elementos: s1){
                String[] separas1 = elementos.split("\\s+");
                total = total + Integer.parseInt(separas1[1]);
            }
            return total;
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> sx1= new ArrayList<>();
            int totalDeSites;
            
            for (Text valores : values) {
                String line = valores.toString();
                sx1.add(line);
            }
            
            totalDeSites = total(sx1);         
            
            for(String elementos: sx1){
                String[] separarElementos = elementos.split("\\s+");
                String freq = "" + (Float.parseFloat(separarElementos[1])/totalDeSites)*100 + " %";
                chave.set(separarElementos[0]);
                result.set(freq);
                context.write(chave, result);
            }          
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Url frequency");
        job.setJarByClass(Urlfrequency.class);
        job.setMapperClass(UrlMapper.class);
        job.setReducerClass(UrlReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("/user/hduser/tempFiles/temp72"));
        job.waitForCompletion(true);
        //segundo map/reduce
        Job job2 = Job.getInstance(conf, "Url frequency2");
        job2.setJarByClass(Urlfrequency.class);
        job2.setMapperClass(UrlMapper2.class);
        job2.setReducerClass(UrlReduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/user/hduser/tempFiles/temp72"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
