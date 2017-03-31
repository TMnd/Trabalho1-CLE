//package com.trabalho1.mutualfriends;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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

/**
 *
 * @author joao
 */
public class MutualFriends {
    
    public static final Log log = LogFactory.getLog(FriendsReducer.class);

    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {

        private static Text listaAmigos = new Text();
        private static Text sujeitos = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String auxSujeitos;
            String line = value.toString(); // exemplo de linha A - > B C D
            log.info("line: " + line);
            String[] separarLinha = line.split("->");

            String sujeito = separarLinha[0];               // exemplo: A
            log.info("sujeito: " + sujeito);
            String[] lista = separarLinha[1].split(",");    // exemplo: B C D
            log.info("lista: " + Arrays.toString(lista));

            for (int i = 0; i < lista.length; i++) {
                if (lista[i].compareTo(sujeito) < 0) {
                    auxSujeitos = lista[i] + " " + separarLinha[0];
                    
                } else {
                    auxSujeitos = separarLinha[0] + " " + lista[i];
                }
                log.info("auxSujeitos: " + auxSujeitos);
                sujeitos.set("("+auxSujeitos + ") ->");
                listaAmigos.set(separarLinha[1]);
                context.write(sujeitos, listaAmigos);
            }
        }
    }

    public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {

        private Text resultadoFinal = new Text();
 

        private String interseccao(String s1, String s2) { 
            ArrayList<String> listaS1 = new ArrayList<String>();
            ArrayList<String> listaS2 = new ArrayList<String>();

            //log.info("s1: " + s1);

            String[] s1Input = s1.split(",");
            for(String lista: s1Input){
                listaS1.add(lista.trim());
            }
            //log.info("First List :" + listaS1);
            
            
            //log.info("s2: " + s2);

            String[] s2Input = s2.split(",");
            for(String lista2: s2Input){
                listaS2.add(lista2.trim());
            }
      
            //log.info("Second List :" + listaS2);
            listaS2.retainAll(listaS1);

            //log.info("After applying the method, First List :" + listaS1);
            //log.info("After applying the method, Second List :" + listaS2);

            log.info("tamanho final: " + listaS2.size());
            if(listaS2.isEmpty()){
                return "/q";
            }else{
                return listaS2.toString();
            }
            
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            log.info("----------------------------------------------------------------------");
            int cont2=0;
            String resultado = null;
            ArrayList<String> valores = new ArrayList<>();
            for(Text value: values){    
                cont2++;
                valores.add(value.toString());        
            }
            log.info("Tamanho de valores: " + valores.size());
            int cont = 0;
            log.info("TESTE PARA VER SE CHEGA AQUI!");
            for(String valoresArrayList: valores){
                log.info("key: " + key + " ---- values: " + valoresArrayList + " ---- cont: " + cont);
                //valores[cont] = value.toString();
                cont++;
            }
            if(valores.size() != 1){
                log.info("Entra na função interseccao");
                resultado = interseccao(valores.get(0), valores.get(1));
                log.info("asdasdsada: " + resultado);
                log.info("asdasdsada: " + resultado.length());
                if(!resultado.equals("/q")){
                    log.info("entrou");
                    resultadoFinal.set(resultado);
                    context.write(key, resultadoFinal);
                }
            }/*else{ //Para imprimir no ficheiro a linha do caso Mark Russell
                log.info("Nao entra na função interseccao");
                resultado = "["+valores.get(0)+"]";
            }
            
            log.info("asdasdsada: " + resultado);
            log.info("asdasdsada: " + resultado.length());
            if(!resultado.equals("/q")){
                log.info("entrou");
                resultadoFinal.set(resultado);
                context.write(key, resultadoFinal);
            }*/
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
