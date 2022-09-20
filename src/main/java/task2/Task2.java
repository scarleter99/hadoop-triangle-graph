package task2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task2(), args);
    }

    public int run(String[] args) throws Exception {

        String inputpath = args[0];
        String tmppath = inputpath + "_task2.tmp";
        String outpath = inputpath + "_task2";

        runTask2Step1(inputpath, tmppath);
        runTask2Step2(tmppath, outpath);

        return 0;
    }

    private void runTask2Step1(String inputpath, String outpath) throws Exception{

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task2.class);

        job.setMapperClass(Task2Step1Mapper.class);
        job.setReducerClass(Task2Step1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));

        job.waitForCompletion(true);

    }

    private void runTask2Step2(String inputpath, String outpath) throws Exception{

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Task2.class);

        job.setMapperClass(Task2Step2Mapper.class);
        job.setReducerClass(Task2Step2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputpath));
        FileOutputFormat.setOutputPath(job, new Path(outpath));

        job.waitForCompletion(true);

    }
}
