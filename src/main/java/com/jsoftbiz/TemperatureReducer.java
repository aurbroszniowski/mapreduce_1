package com.jsoftbiz;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: This will get as entry a key2, List<value2>
 *   key2 is the state, the List is the list of temperatures for that state
 *   The reducer does the logic, it calculates the average and output a key3, value3 where
 *   key3 is the state and value3 is the sverage temperature
 *
 * @author Aurelien Broszniowski
 */
public class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
    int sumOfTemperatures = 0;
    int nbValues = 0;
    for (IntWritable temperature : values) {
      sumOfTemperatures += temperature.get();
      nbValues++;
    }
    int average = sumOfTemperatures / nbValues;
    context.write(key, new IntWritable(average));
  }
}
