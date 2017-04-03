//package com.trabalho1.mutualfriends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class MutualFriends {
    
    public static final Log log = LogFactory.getLog(FriendsReducer.class); // Para imprimir na consola

    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {

        private static Text listaAmigos = new Text();
        private static Text sujeitos = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String auxSujeitos;
            String line = value.toString(); // exemplo de linha A - > B C D
            String[] separarLinha = line.split("->");

            String sujeito = separarLinha[0];               // exemplo: A
            String[] lista = separarLinha[1].split(",");    // exemplo: B C D

            for (int i = 0; i < lista.length; i++) {
                if (lista[i].compareTo(sujeito) < 0) {
                    auxSujeitos = lista[i] + " " + separarLinha[0];
                    
                } else {
                    auxSujeitos = separarLinha[0] + " " + lista[i];
                }
                sujeitos.set("("+auxSujeitos + ") ->");
                listaAmigos.set(separarLinha[1]);
                context.write(sujeitos, listaAmigos);
            }
        }
    }

    public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {

        private Text resultadoFinal = new Text();
        
        private String soloEntry(String chave, String entry){
            ArrayList<String> listaEntry = new ArrayList<String>();

            String[] s1Input = entry.split(",");
            for(String lista: s1Input){
                listaEntry.add(lista.trim());
            }
            
            String[] chaveSplit = chave.split(" ");
           
            for(int j=0; j<listaEntry.size();j++){
                for(int i=0; i<chaveSplit.length-1;i++){
                    if(chaveSplit[i].replace("(", "").replace(")","").equals(listaEntry.get(j))){
                        listaEntry.remove(chaveSplit[i].replace("(", "").replace(")",""));
                    }
                }
            }

            return listaEntry.toString();
        }
 

        private String interseccao(String chave,String s1, String s2) { 
            ArrayList<String> listaS1 = new ArrayList<String>(); //Para conter a lista da primeira pessoa a comparar
            ArrayList<String> listaS2 = new ArrayList<String>(); //Para conter a lista da segunda pessai a comprar

            String[] s1Input = s1.split(",");
            for(String lista: s1Input){
                listaS1.add(lista.trim());
            }

            String[] s2Input = s2.split(",");
            for(String lista2: s2Input){
                listaS2.add(lista2.trim());
            }
      
            listaS2.retainAll(listaS1);   //Remove na listas2 os elementos que nao existe na listaS1

            if(listaS2.isEmpty()){
                return "/q";
            }else{
                return listaS2.toString();
            }
            
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cont2=0; //Para verificar quatas pessoas sao para comparar
            String resultado = null;
            ArrayList<String> valores = new ArrayList<>();
            for(Text value: values){    
                cont2++;
                valores.add(value.toString());        
            }
            if(valores.size() != 1){
                resultado = interseccao(key.toString(),valores.get(0), valores.get(1));
                if(!resultado.equals("/q")){
                    resultadoFinal.set(resultado);
                    context.write(key, resultadoFinal);
                }
            }else{ //Para imprimir no ficheiro a linha do caso Mark Russell em que aparecem os amigos SEM o Mark visto que ele é uma pessoa a comparar (mark é amigo de russel mas russel nao é amigo do mark)
                resultado = soloEntry(key.toString(), valores.get(0));
                resultadoFinal.set(resultado);
                context.write(key, resultadoFinal);
            }
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
