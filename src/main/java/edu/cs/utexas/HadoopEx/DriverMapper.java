package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.curator.shaded.com.google.common.util.concurrent.Monitor;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DriverMapper extends Mapper<Object, Text, Text, TupWritable> {

    private FloatWritable money;
    private IntWritable seconds;
	// Create a hadoop text object to store words
	private Text driverID = new Text();
    static private int NUM_ATTR = 17;
    static private float SMALL_ERROR = 0.001f;

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] attr = value.toString().split(",");

        // set float writables

        // correct num commas
        if (attr.length != NUM_ATTR) {
            return;
        }

        // all money values are float and valid
        try {
            Float calc_total = Float.parseFloat(attr[11]) + Float.parseFloat(attr[12]) + Float.parseFloat(attr[13]) + Float.parseFloat(attr[14]) + Float.parseFloat(attr[15]);
            Integer total_time = Integer.parseInt(attr[4]);
            if (Math.abs(calc_total - Float.parseFloat(attr[16])) > SMALL_ERROR || calc_total - 500.0 > SMALL_ERROR || total_time == 0) {
                return;
            }
            
            // Set money and seconds
            money = new FloatWritable(calc_total);   
            seconds = new IntWritable(total_time);
        } catch (NumberFormatException e) {
            return;
        }

        // set driverID
        driverID.set(attr[1]);

        Writable[] valueout = {money, seconds};
        TupWritable tuple = new TupWritable(valueout);
        context.write(driverID, tuple);
	}
}