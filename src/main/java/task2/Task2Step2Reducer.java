package task2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Step2Reducer extends Reducer<Text, Text, Text, Text>{

    Text ok = new Text();
    Text ov = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        StringTokenizer st = new StringTokenizer(key.toString());

        String n1 = st.nextToken();
        String n2 = st.nextToken();

        ArrayList<String> degrees = new ArrayList<String>();
        for(Text v2: values) {
            degrees.add(v2.toString());
        }

        int[] degreePair = new int[2];
        for (String degree : degrees) {
            StringTokenizer st2 = new StringTokenizer(degree.toString());

            String n1Degree = st2.nextToken();
            String n2Degree = st2.nextToken();

            if (n1Degree.equals("-1")) {
                degreePair[1] = Integer.parseInt(n2Degree);
            } else {
                degreePair[0] = Integer.parseInt(n1Degree);            }
        }

        if (degreePair[0] <= degreePair[1]) {
            ok.set(n1);
            ov.set(n2);
            context.write(ok, ov);
        } else  {
            ok.set(n2);
            ov.set(n1);
            context.write(ok, ov);
        }

    }
}
