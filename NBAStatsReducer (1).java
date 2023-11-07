package stats;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NBAStatsReducer extends Reducer<Text, Text, Text, Text> {

	private Text outputValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            outputValue.set(value.toString());
            context.write(key, outputValue);
        }
    }
}
