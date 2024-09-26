package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class TaxiTopKReducer extends  Reducer<Text, FloatWritable, Text, FloatWritable> {

    private static int K = 5;
    private PriorityQueue<WordAndCount> pq = new PriorityQueue<WordAndCount>(K);


    private Logger logger = Logger.getLogger(TaxiTopKReducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<WordAndCount>(K);
//    }


    /**
     * Takes in the topK from each mapper and calculates the overall topK
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<FloatWritable> ratios, Context context)
           throws IOException, InterruptedException {


       // A local counter just to illustrate the number of values here!
        int counter = 0 ;


       // size of values is 1 because key only has one distinct value
       for (FloatWritable ratio : ratios) {
           counter = counter + 1;
           logger.info("Reducer Text: counter is " + counter);
           logger.info("Reducer Text: Add this item  " + new WordAndCount(key, ratio).toString());

           pq.add(new WordAndCount(new Text(key), new FloatWritable(ratio.get()) ) );

           logger.info("Reducer Text: " + key.toString() + " , Count: " + ratio.toString());
           logger.info("PQ Status: " + pq.toString());
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > K) {
           pq.poll();
       }


   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("TopKReducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<WordAndCount> values = new ArrayList<WordAndCount>(K);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (WordAndCount value : values) {
            context.write(value.getWord(), value.getRatio());
            logger.info("TopKReducer - Top-K Words are:  " + value.getWord() + "  Count:"+ value.getRatio());
        }


    }

}