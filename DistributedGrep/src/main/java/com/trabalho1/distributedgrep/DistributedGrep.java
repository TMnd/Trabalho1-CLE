//package com.trabalho1.distributedgrep;

import java.io.IOException;
import java.util.StringTokenizer;   
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

public class DistributedGrep {
    
    public static final Log log = LogFactory.getLog(GrepMapper.class); // Para imprimir na consola

    public static class GrepMapper extends Mapper <Object, Text, Text, Text>{
        private Text valor = new Text();     
        private Text word = new Text();     
    
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{ 
            Configuration conf = context.getConfiguration();
            String param = conf.get("regex");
            Pattern p = Pattern.compile(param);
            String line = value.toString();
                       
            Matcher m = p.matcher(line);
            
            if(m.find()){
                word.set(line);
                valor.set("");
                context.write(word,valor);
            }
        }
    }
    
    public static class GrepReduce extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text(); 
        public void reduce (Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            result.set("");
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("regex", args[2]);
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(DistributedGrep.class);
        job.setMapperClass(GrepMapper.class);
        job.setReducerClass(GrepReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
