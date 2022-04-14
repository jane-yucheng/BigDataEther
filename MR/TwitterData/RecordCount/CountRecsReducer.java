import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class CountRecsReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

 
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int valueSum = 0;
        int lVal = 0;
        // System.out.println(values);
        for (IntWritable val: values) 
            valueSum+=val.get();
            

        // count.set(value);
        System.out.println(valueSum);
        context.write(key, new IntWritable(valueSum));
    }

}
