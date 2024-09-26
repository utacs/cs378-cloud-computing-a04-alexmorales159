package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TaxiTopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	private Logger logger = Logger.getLogger(TaxiTopKMapper.class);
	private static int K = 5;

	private PriorityQueue<WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text tuple, Context context)
			throws IOException, InterruptedException {

		String strTuple = tuple.toString();
		// cut off '(' and ')'
		strTuple = strTuple.substring(1, strTuple.length() - 1);
		String[] tup_vals = strTuple.split(",");

		int errorSum = Integer.parseInt(tup_vals[0]);
		int totalSum = Integer.parseInt(tup_vals[1]);
		float errorRatio = ((float)errorSum) / ((float)totalSum);

		pq.add(new WordAndCount(new Text(key), new FloatWritable(errorRatio)) );

		if (pq.size() > K) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getErrorRatio());
			logger.info("TaxiTopKMapper PQ Status: " + pq.toString());
		}
	}

}