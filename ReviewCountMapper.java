import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text bookTitle = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Skip header line
        if (key.get() == 0 && value.toString().contains("Id")) {
            return;
        }

        String[] fields = value.toString().split(",", -1); // keep empty strings
        if (fields.length > 1) {
            String title = fields[1].trim();
            if (!title.isEmpty()) {
                bookTitle.set(title);
                context.write(bookTitle, one);
            }
        }
    }
}
