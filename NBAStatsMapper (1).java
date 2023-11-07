package stats;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NBAStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private boolean firstLine = true;
    private String columnHeadings;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if (firstLine) {
            columnHeadings = value.toString();
            // Remove the additional column heading
            columnHeadings = columnHeadings.replace(",Player-additional", "");
            firstLine = false;
            return;
        }

        String[] line = value.toString().split(",");
        // Remove the additional column value
        line = Arrays.copyOf(line, line.length - 1);

        // Exclude the first two columns (Rk and Player)
        String[] stats = Arrays.copyOfRange(line, 2, line.length);

        // Format the player's name
        String playerName = line[1].replaceAll("\\s+", " ");

        // Format the stats
        String[] columns = columnHeadings.split(",", 3);
        String formattedStats = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                stats[0], stats[1], stats[2], stats[3], stats[4],
                stats[5], stats[6], stats[7], stats[8], stats[9], stats[10],
                stats[11], stats[12], stats[13], stats[14], stats[15],
                stats[16], stats[17], stats[18], stats[19], stats[20],
                stats[21], stats[22], stats[23], stats[24], stats[25], stats[26], stats[27]);

        outputKey.set(playerName);
        outputValue.set(formattedStats);

        context.write(outputKey, outputValue);
    }

    
}

