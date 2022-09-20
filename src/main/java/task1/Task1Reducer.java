package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<Text, Text, Text, Text>{

    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        ov.set("");
        context.write(key, ov);
    }
}
