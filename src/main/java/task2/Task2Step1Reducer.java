package task2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Step1Reducer extends Reducer<Text, Text, Text, Text>{

    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        ArrayList<String> edges = new ArrayList<String>();
        for(Text v : values) {
            edges.add(v.toString());
        }

        int degree = edges.size();

        for (String edge : edges) {
            String[] node =  edge.split(" ");

            if (node[0].equals(key.toString())) {
                ov.set(degree + " -1");
            } else {
                ov.set("-1 " + degree);
            }

            ok.set(edge);
            context.write(ok, ov);
        }
    }
}
