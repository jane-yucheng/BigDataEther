import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        String[] line = value.toString().split(",");
	
        StringBuilder str =  new StringBuilder();

        String date = line[0].split(" ")[0];
        String sales  = line[2];

        str.append(date.replace("\"","").trim());
        str.append(",");
        str.append(sales);

	if (date=="DateTime"){
		return;
	}        
        context.write(new Text(""), new Text(str.toString()));
            
        }
}

