package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/*
 *  Output:     (hour of day, 1)
 *  Hour needs to be between 1-24
 */
public class GPSErrorsMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
    // parser to convert string to Date (tp grab hour)
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
    static private int NUM_ATTR = 17;
    static private float SMALL_ERROR = 0.001f;

	public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
        String[] attr = value.toString().split(",");

        // boolean invalid = false;    // only add to GPS errors if rest of line is valid

        // correct num commas
        if (attr.length != NUM_ATTR) {
            // invalid = true;
            return;
        }

        // all money values are float and valid
        try {
            Float calc_total = Float.parseFloat(attr[11]) + Float.parseFloat(attr[12]) + Float.parseFloat(attr[13]) + Float.parseFloat(attr[14]) + Float.parseFloat(attr[15]);
            Integer total_time = Integer.parseInt(attr[4]);
            if (Math.abs(calc_total - Float.parseFloat(attr[16])) > SMALL_ERROR || calc_total - 500.0 > SMALL_ERROR || total_time == 0) {
                // invalid = true;
                return;
            }
        } catch (/*IndexOutOfBoundsException |*/ NumberFormatException e) {
            // invalid = true;
            return;
        }

        String pickup_datetime = attr[2];
        String dropoff_datetime = attr[3];
        // NEED TO CHANGE LONGITUDE AND LATITUDE DEFAULTS VALUES
        int a_longitude = 0;
        try {
            a_longitude = Integer.parseInt(attr[6]);
        } catch (NumberFormatException e) {
            if (attr[6].length() == 0) {
                a_longitude = 0;
            }
        }
        int a_latitude = 0;
        try {
            a_latitude = Integer.parseInt(attr[7]);
        } catch (NumberFormatException e) {
            if (attr[7].length() == 0) {
                a_latitude = 0;
            }
        }
        int b_longitude = 0;
        try {
            b_longitude = Integer.parseInt(attr[8]);
        } catch (NumberFormatException e) {
            if (attr[8].length() == 0) {
                b_longitude = 0;
            }
        }
        int b_latitude = 0;
        try {
            b_latitude = Integer.parseInt(attr[9]);
        } catch (NumberFormatException e) {
            if (attr[9].length() == 0) {
                b_latitude = 0;
            }
        }

        if (/*!invalid && */(a_longitude == 0 || a_latitude == 0)) {
            // GPS pickup error
            try {
                LocalDateTime dateTime = LocalDateTime.parse(pickup_datetime, formatter);
                IntWritable hour = new IntWritable(dateTime.getHour() + 1); // getHour returns 0-23, but we need 1-24
                context.write(hour, counter);
            } catch (DateTimeParseException e) {
                e.printStackTrace();
            }
        }
        if (/*!invalid && */(b_longitude == 0 || b_latitude == 0)) {
            // GPS dropoff error
            try {
                LocalDateTime dateTime = LocalDateTime.parse(dropoff_datetime, formatter);
                IntWritable hour = new IntWritable(dateTime.getHour() + 1); // getHour returns 0-23, but we need 1-24
                context.write(hour, counter);
            } catch (DateTimeParseException e) {
                e.printStackTrace();
            }
        }
    }

}