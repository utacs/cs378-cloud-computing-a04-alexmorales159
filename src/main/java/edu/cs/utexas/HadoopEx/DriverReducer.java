package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DriverReducer extends  Reducer<Text, TupWritable, Text, TupWritable> {

   public void reduce(Text text, Iterable<TupWritable> tuples, Context context)
           throws IOException, InterruptedException {
	   
        float moneySum = 0.0f;
        int secondSum = 0;
        for (TupWritable tuple : tuples) {
            moneySum += ((FloatWritable) tuple.get(0)).get();
            secondSum += ((IntWritable) tuple.get(1)).get();
        }

        Writable[] reducedout = {new FloatWritable(moneySum), new IntWritable(secondSum)};
        context.write(text, new TupWritable(reducedout));
   }
}