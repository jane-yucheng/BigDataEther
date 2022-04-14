import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
          //split line by comma
        if(value.toString().startsWith("id")){
            return;
        }
        String[] line = value.toString().split(",", -1);
        // //check to see if it is the proper length(10)
        // int num = line.length;
        // if(num == 16){
        //     // if it is retrieve the neighborhood group and neighborhood

        //     String neighborhood = line[4]+ " "+ line[5];
        //     context.write(new Text(neighborhood), new IntWritable(1));


        // }
        String date = line[2];
        String tweets = line[11];
        if(tweets.length()<1 || date.equals("1970/01/01") || tweets.equals("null")){
            return;
        }
        else{
//            context.write(new Text("wrong length"), new IntWritable(num));
//            context.write(new Text(line), new IntWritable(num));
            context.write(new Text(date), new IntWritable(Integer.parseInt(tweets)));
            return;
        }
            
        }
}

