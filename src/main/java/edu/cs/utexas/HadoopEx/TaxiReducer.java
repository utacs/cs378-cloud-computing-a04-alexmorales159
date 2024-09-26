package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiReducer extends  Reducer<Text, TupWritable, Text, TupWritable> {

   public void reduce(Text text, Iterable<TupWritable> tuples, Context context)
           throws IOException, InterruptedException {
	   
        int errorSum = 0;
        int totalSum = 0;
        for (TupWritable tuple : tuples) {
            errorSum += ((IntWritable) tuple.get(0)).get();
            totalSum += ((IntWritable) tuple.get(1)).get();
        }

        Writable[] reducedout = {new IntWritable(errorSum), new IntWritable(totalSum)};
        context.write(text, new TupWritable(reducedout));
   }
}