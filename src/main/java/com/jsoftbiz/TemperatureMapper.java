package com.jsoftbiz;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapper. This will get as entry a key1 and value1
 * key1 is the city name. We are going to discard it.
 * value1 is a String containing the state and temperature. We are going to parse them and output
 * key2=state, value2=temperature
 *
 * @author Aurelien Broszniowski
 */
public class TemperatureMapper extends Mapper<Text, String, Text, IntWritable> {

  @Override
  public void map(Text key, String value, Context context) throws IOException, InterruptedException {
    if (isValueValid(value)) {
      Text key2 = new Text(getStateFromValue(value));
      IntWritable value2 = new IntWritable(getTemperatureFrom(value));
      context.write(key2, value2);
    }
  }

  private boolean isValueValid(final String value) {
    // We expect that the value is a String in the form of : State, Temperature. E.g. MP,77
    Pattern p = Pattern.compile("\\S\\S\\,\\d+");
    Matcher m = p.matcher(value);
    return m.matches();
  }

  private String getStateFromValue(final String value) {
    final String[] subvalues = value.split("\\,");
    return subvalues[0];
  }

  private int getTemperatureFrom(final String value) {
    final String[] subvalues = value.split("\\,");
    return Integer.parseInt(subvalues[1]);
  }
}
