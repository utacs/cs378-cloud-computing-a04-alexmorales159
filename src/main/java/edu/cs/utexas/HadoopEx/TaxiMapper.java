package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiMapper extends Mapper<Object, Text, Text, TupWritable> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
    // Create a hadoop IntWritable object to store error with default value 0
    private IntWritable error = new IntWritable(0);
	// Create a hadoop text object to store words
	private Text taxiID = new Text();
    static private int NUM_ATTR = 17;
    static private float SMALL_ERROR = 0.001f;

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] attr = value.toString().split(",");
        error = new IntWritable(0);

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
        } catch (NumberFormatException e) {
            return;
        }

        // set taxiID
        taxiID.set(attr[0]);
        // NEED TO CHANGE LONGITUDE AND LATITUDE DEFAULTS VALUES
        float a_longitude = -1;
        try {
            a_longitude = Float.parseFloat(attr[6]);
        } catch (NumberFormatException e) {
            if (attr[6].length() == 0) {
                a_longitude = 0f;
            } else {
                return;
            }
        }
        float a_latitude = -1;
        try {
            a_latitude = Float.parseFloat(attr[7]);
        } catch (NumberFormatException e) {
            if (attr[7].length() == 0) {
                a_latitude = 0f;
            } else {
                return;
            }
        }
        float b_longitude = -1;
        try {
            b_longitude = Float.parseFloat(attr[8]);
        } catch (NumberFormatException e) {
            if (attr[8].length() == 0) {
                b_longitude = 0f;
            } else {
                return;
            }
        }
        float b_latitude = -1;
        try {
            b_latitude = Float.parseFloat(attr[9]);
        } catch (NumberFormatException e) {
            if (attr[9].length() == 0) {
                b_latitude = 0f;
            } else {
                return;
            }
        }

        if (Float.compare(a_longitude, 0f) == 0 || Float.compare(a_latitude, 0f) == 0 || Float.compare(b_longitude, 0f) == 0 || Float.compare(b_latitude, 0f) == 0) {
            // GPS pickup error
            error = new IntWritable(1);
        }
        Writable[] valueout = {error, counter};
        TupWritable tuple = new TupWritable(valueout);
        context.write(taxiID, tuple);
	}
}
