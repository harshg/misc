import java.io.IOException;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MrSortMapper extends MapReduceBase
    implements Mapper<Text, Text, Text, Text> {

  private final Text blank = new Text("");

  public void map(Text key, Text value,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	  output.collect(key, blank);
  }
}