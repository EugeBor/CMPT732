// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import java.util.regex.Pattern;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			Text date_time = new Text();
			Text page_name = new Text();
			LongWritable hits = new LongWritable();

			String [] vals = value.toString().split(" ");

			date_time.set(vals[0]);
			String language = vals[1];

			if (language.equals("en")) {
				String page = vals[2];
				if (!page.startsWith("Special:") && !page.equals("Main_Page")) {

					page_name.set(vals[2]);	
					Long hitsLong = new Long(vals[3]);
					hits.set(hitsLong.longValue());
					System.out.println("Writing " + date_time + " with " + hits);
					context.write(date_time, hits);
				}
			}				
		}
	}

	public static class Wikipedia_Reducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {

			long max_hits = 0L;
			System.out.println("reduce start - key " + key);
			for (LongWritable val : values){
				System.out.println("key: " + key + ", value: " + val);
				if (val.get() > max_hits) {
					max_hits = val.get();
				}
			}
			System.out.println("Complete. Key: " + key + " max_hits " + max_hits);
			context.write(key, new LongWritable(max_hits));
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia_popular");
		job.setJarByClass(WikipediaPopular.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(Wikipedia_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}