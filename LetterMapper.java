package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mongodb.MongoClient;

public class LetterMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String val = value.toString();
	  String arr[] = val.split(" ", 2);
	  if (arr.length < 2) arr = val.split("\\t", 2);
	  context.write(new Text(arr[0]), new Text(arr[1]));
  }
}
