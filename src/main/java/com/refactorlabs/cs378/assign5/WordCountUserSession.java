package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCountUserSession extends Configured implements Tool {

	public final static LongWritable ONE = new LongWritable(1L);

	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] fields = line.split("\\t");


            String fullType = fields[1];
            int s = fullType.indexOf(' ');
            String eventType = fullType.substring(0, s);
            String eventSubtype = fullType.substring(s + 1, fullType.length());

            // Event type
            word.set("eventType:"+eventSubtype);
            context.write(word, ONE);

            // Event subtype
			word.set("eventSubtype:"+eventSubtype);
			context.write(word, ONE);

            // Page
            word.set("page:"+fields[2]);
            context.write(word, ONE);

            // City
            word.set("city:"+fields[5]);
            context.write(word, ONE);

            // Vehicle condition
            word.set("vehicleCondition:"+fields[7]);
            context.write(word, ONE);

            // Make
            word.set("make:"+fields[9]);
            context.write(word, ONE);

            // Model
            word.set("model:"+fields[10]);
            context.write(word, ONE);

            // Trim
            word.set("trim:"+fields[11]);
            context.write(word, ONE);

			// Body style
			word.set("bodyStyle:"+fields[12]);
			context.write(word, ONE);

			// Cab style
			word.set("cabStyle:"+fields[13]);
			context.write(word, ONE);

            // Carfax free report
            word.set("carfaxFreeReport:"+fields[17]);
            context.write(word, ONE);

            // Features
            word.set("features:"+fields[18]);
            context.write(word, ONE);
		}
	}

	public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;

			// Sum up the counts for the current word, specified in object "key".
			for (LongWritable value : values) {
				sum += value.get();
			}
			// Emit the total count for the word.
			context.write(key, new LongWritable(sum));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "WordCountUserSession");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordCountUserSession.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		int res = ToolRunner.run(new WordCountUserSession(), args);
		System.exit(res);
	}
}
