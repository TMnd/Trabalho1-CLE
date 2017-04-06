//package com.trabalho1.sorting;

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;   
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
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

/**
 *
 * @author joao
 */
public class Sorting {
    
    public static final Log log = LogFactory.getLog(SortingMapper.class);

    
    public static class SortingMapper extends Mapper <Object, Text, Text, Text>{
        private Text word = new Text();
        private Text wordk = new Text("");
    
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            word.set(line);
            wordk.set("");
            context.write(wordk,word);
        }
    }
    
    public static class SortingReduce extends Reducer<Text, Text, Text, Text>{ 
        private Text resultado = new Text();
        
        private ArrayList sorting(ArrayList<String> ar) { 
            Collections.sort(ar, new Comparator<String>() {
                @Override
                public int compare(String s1, String s2)  {
                    return  s1.compareTo(s2);
                }
            });       
            
            return ar;
        }
        
        public void reduce (Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            ArrayList<String> lista = new ArrayList<>();
            ArrayList<String> listaSorted = new ArrayList<>();
            for(Text valores : values){
                String line = valores.toString();
                lista.add(line);
            }
            
            listaSorted=sorting(lista);
            
            for(String valores : listaSorted){
                resultado.set(valores);
                context.write(resultado,key);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sorting");
        job.setJarByClass(Sorting.class);
        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
