package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Task3_1Test {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInt("mapreduce.job.reduces", 3);

        String[] params = {"src/test/resources/fb.txt_task1"};

        ToolRunner.run(conf, new Task3(), params);
    }
}