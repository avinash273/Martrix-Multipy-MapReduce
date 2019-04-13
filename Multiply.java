package com.matrix.adb;
/*
CSE-6331-005
Project 2
Matrix Multiplication Program
Written by,
Avinash Shanker
Roll No: 1001668570
UTA ID: AXS8570
Date: 24-Feb-2019
*/

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
    int tag;
    int index;
    double value;

    public Elem () {}

    public Elem (int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    public void write (DataOutput out) throws IOException{
        out.writeInt(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    public void readFields (DataInput in) throws IOException{
        tag = in.readInt();
        index = in.readInt();
        value = in.readDouble();
    }


}

class Pair implements WritableComparable<Pair>{
    int i;
    int j;

    public Pair() {}

    public Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public void write (DataOutput out) throws IOException{
        out.writeInt(i);
        out.writeInt(j);
    }

    public void readFields (DataInput in) throws  IOException{
        i = in.readInt();
        j = in.readInt();
    }

    public int compareTo (Pair comp) {
        if(i>comp.i)
            return 1;
        else if(i<comp.i)
            return -1;
        else{
            if(j>comp.j)
                return 1;
            else if(j<comp.j)
                return -1;
        }
        return 0;
    }

    public String toString() {
        String str = i + " " + j + " ";
        return  str;
    }
}

public class Multiply  {

    public static class MatrixMapM extends Mapper<Object,Text,IntWritable,Elem>{
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            String readLine = value.toString();
            String[] stringTokens = readLine.split(",");
            int i = Integer.parseInt(stringTokens[0]);
            int j = Integer.parseInt(stringTokens[1]);
            double v = Double.parseDouble(stringTokens[2]);
            context.write(new IntWritable(j),new Elem(0,i,v));
        }
    }

    public static class MatrixMapN extends Mapper<Object,Text,IntWritable,Elem>{
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            String readLine = value.toString();
            String[] stringTokens = readLine.split(",");
            int i = Integer.parseInt(stringTokens[0]);
            int j = Integer.parseInt(stringTokens[1]);
            double v = Double.parseDouble(stringTokens[2]);
            context.write(new IntWritable(i),new Elem(1,j,v));
        }
    }

    public static class ResultReducer extends Reducer <IntWritable, Elem, Pair, DoubleWritable>{

        @Override
        public void reduce(IntWritable key, Iterable<Elem> values, Context context)
            throws IOException, InterruptedException {
            ArrayList<Elem> M = new ArrayList<Elem>();
            ArrayList<Elem> N = new ArrayList<Elem>();
            Configuration config = context.getConfiguration();

            for (Elem Elem : values) {
                Elem tEle = ReflectionUtils.newInstance(Elem.class, config);
                ReflectionUtils.copy(config,Elem,tEle);
                if(tEle.tag == 0)
                {
                    M.add(tEle);
                }
                else if(tEle.tag ==1)
                {
                    N.add(tEle);
                }
            }

            for (int i=0;i<M.size();i++)
            {
                for (int j=0;j<N.size();j++)
                {
                    Pair one = new Pair(M.get(i).index,N.get(j).index);
                    double mvalue = M.get(i).value;
                    double nvalue = N.get(j).value;
                    double multiply = mvalue*nvalue;
                    context.write(one, new DoubleWritable(multiply));
                }
            }
        }
    }

    public static class EmptyMap extends Mapper<Object,Text,Pair,DoubleWritable>
    {
        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String readLine = value.toString();
            String[] pval = readLine.split(" ");
            context.write(new Pair(Integer.parseInt(pval[0]),Integer.parseInt(pval[1])),new DoubleWritable(Double.parseDouble(pval[2])));
        }
    }

    public static class SumReduce extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable>
    {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException
        {
            double m =0;
            for (DoubleWritable value : values)
            {
                m = m+value.get();
            }
            context.write(key,new DoubleWritable(m));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Job job1 = Job.getInstance();
        job1.setJobName("MapReduce1");
        Process OutputRemove1 = Runtime.getRuntime().exec("rm -r output1");
        Process OutputRemove2 = Runtime.getRuntime().exec("rm -r output2");
        job1.setJarByClass(Multiply.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MatrixMapM.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MatrixMapN.class);
        job1.setReducerClass(ResultReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("MapReduceSum");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(EmptyMap.class);
        job2.setReducerClass(SumReduce.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
/*
References:
http://lambda.uta.edu/cse6331/
http://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
https://github.com/shask9/Matrix-Multiplication-Hadoop/blob/master/Multiply.java
*/