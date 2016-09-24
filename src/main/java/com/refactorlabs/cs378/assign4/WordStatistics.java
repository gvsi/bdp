package com.refactorlabs.cs378.assign4;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
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
import java.util.HashMap;
import java.util.StringTokenizer;

public class WordStatistics extends Configured implements Tool {

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().toLowerCase()
					.replaceAll("(\\[\\d+\\])", " $1 ") // Add a space before and after [number], so it's tokenized
					.replaceAll("\\.|,|--|;|:|\\?|\"|'|=|_|!|\\(|\\)", " "); // Remove punctuation

			StringTokenizer tokenizer = new StringTokenizer(line);

			// A hashmap that keeps track of word frequencies in this paragraph
			HashMap<String, Integer> freq = new HashMap<>();

			// For each word in the input line, emit a count of 1 for that word.
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();

				if (freq.containsKey(word)) {
					freq.put(word, freq.get(word) + 1);
				} else {
					freq.put(word, 1);
				}
			}

			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();

			int paragraph_length = 0;

			for (String word : freq.keySet()) {
				int f = freq.get(word);
				builder.setDocCount(1L);
				builder.setTotalCount(f);
				builder.setSumOfSquares(f * f);
				builder.setMin(f);
				builder.setMax(f);

				paragraph_length += freq.get(word);

				context.write(new Text(word), new AvroValue<WordStatisticsData>(builder.build()));
			}

			// Set paragraph statistics
			builder.setDocCount(1);
			builder.setTotalCount(paragraph_length);
			builder.setSumOfSquares(paragraph_length * paragraph_length);

			// Emit paragraph statistics (first char is ÷ so that is shows up at the end)
			context.write(new Text("÷ParagraphStats"), new AvroValue<WordStatisticsData>(builder.build()));

		}
	}


	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class sums up each component of a word's stats, and then computes mean and variance.
	 */
	public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {
			long paragraph_sum = 0L;
			long freq_sum = 0;
			long freq_squared_sum = 0;
			double mean;
			double variance;
			long min = Long.MAX_VALUE;

			long max = Long.MIN_VALUE;

			// Sum up the counts for the current word, specified in object "key".
			for (AvroValue<WordStatisticsData> value : values) {

				paragraph_sum += value.datum().getDocCount();
				freq_sum += value.datum().getTotalCount();
				freq_squared_sum += value.datum().getSumOfSquares();

				if (value.datum().getMin() < min)
					min = value.datum().getMin();
				if (value.datum().getMax() > max)
					max = value.datum().getMax();
			}

			mean = (double) freq_sum / paragraph_sum;
			variance = (double) freq_squared_sum / paragraph_sum - mean * mean;

			// Emit the total count for the word.
			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
			builder.setDocCount(paragraph_sum);
			builder.setTotalCount(freq_sum);
			builder.setMin(min);
			builder.setMax(max);
			builder.setSumOfSquares(freq_squared_sum);
			builder.setMean(mean);
			builder.setVariance(variance);
			context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Use this JAR first in the classpath (We also set a bootstrap script in AWS)
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

		Job job = Job.getInstance(conf, "WordStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

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
		int res = ToolRunner.run(new WordStatistics(), args);
		System.exit(res);
	}
}
