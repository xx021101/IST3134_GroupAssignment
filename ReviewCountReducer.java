import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Integer> countMap = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        countMap.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort by value descending
        countMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(10) // Top 10
                .forEach(e -> {
                    try {
                        context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
                    } catch (IOException | InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                });
    }
}
