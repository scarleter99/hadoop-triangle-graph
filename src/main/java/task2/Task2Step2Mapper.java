package task2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Step2Mapper extends Mapper<Object, Text, Text, Text> {

    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        //String[] v = value.toString().split("\t\t");

        StringTokenizer st = new StringTokenizer(value.toString());

//        int u = Integer.parseInt(st.nextToken());
//        int v = Integer.parseInt(st.nextToken());

        ok.set(st.nextToken() + " " + st.nextToken());
        ov.set(st.nextToken() + " " + st.nextToken());
        context.write(ok, ov);
    }
}