package task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TriStep2MapperForWedges extends Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
	
	@Override
	protected void map(IntPairWritable key, IntWritable value, Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
