package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/*
 *  Output:     (hour of day, number of errors)
 */
public class GPSErrorsMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
    // parser to convert string to Date (tp grab hour)
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
        String[] myVal = value.toString().split(",");

        String departure = myVal[2];
        int a_longitude = Integer.parseInt(myVal[6]);
        int a_latitude = Integer.parseInt(myVal[7]);
        int b_longitude = Integer.parseInt(myVal[8]);
        int b_latitude = Integer.parseInt(myVal[9]);

        if (a_longitude == 0 || a_latitude == 0 || b_longitude == 0 || b_latitude == 0) {
            // GPS error
            try {
                LocalDateTime dateTime = LocalDateTime.parse(departure, formatter);
                IntWritable hour = new IntWritable(dateTime.getHour() + 1); // getHour returns 0-23, but we need 1-24
                context.write(hour, new IntWritable(1));
            } catch (DateTimeParseException e) {
                e.printStackTrace();
            }
        }
    }

}