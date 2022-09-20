package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Task1Test {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 3);

        String[] params = {"src/test/resources/fb.txt"};

        ToolRunner.run(conf, new Task1(), params);
    }
}