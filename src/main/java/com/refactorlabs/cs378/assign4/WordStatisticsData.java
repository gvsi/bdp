package com.refactorlabs.cs378.assign2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatisticsData {

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		/**
		 * Local variable "stats" will contain statistics for a specific word.
		 * It is of type WordStatisticsWritable, where:
		 * paragraphCount -> Number of paragraphs containing the word (one)
		 * mean -> Number of occurrences of the word in the paragraph
		 * variance -> Number of occurrences of the word in the paragraph squared (needed for variance)
		 */
		private WordStatisticsWritable stats = new WordStatisticsWritable();

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

			int paragraph_length = 0;

			for (String word : freq.keySet()) {
				// count -> Number of paragraphs containing the word
				// mean -> Number of occurrences of the word in the paragraph
				// variance -> Number of occurrences of the word in the paragraph squared (needed for variance)

				stats.setParagraphCount(1);
				stats.setMean(freq.get(word));
				stats.setVariance(Math.pow(freq.get(word), 2));

				paragraph_length += freq.get(word);

				context.write(new Text(word), stats);
			}

			// Set paragraph statistics
			stats.setParagraphCount(1);
			stats.setMean(paragraph_length);
			stats.setVariance(paragraph_length * paragraph_length);

			// Emit paragraph statistics (first char is ÷ so that is shows up at the end)
			context.write(new Text("÷ParagraphStats"), stats);

		}
	}

	/**
	 * The Combine class for word statistics.  Extends class Reducer, provided by Hadoop.
	 * This class combines the stats from each world by summing up each component
	 * and then emitting the result as a single WordStatisticsWritable
	 */
	public static class CombineClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		/**
		 * Counter group for the combiner.  Individual counters are grouped for the reducer.
		 */

		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {

			WordStatisticsWritable stats = new WordStatisticsWritable();
			long paragraph_sum = 0L;
			double freq_sum = 0.0;
			double freq_squared_sum = 0.0;

			// Sum up the counts for the current word, specified in object "key".
			for (WordStatisticsWritable value : values) {
				paragraph_sum += value.getParagraphCount();
				freq_sum += value.getMean();
				freq_squared_sum += value.getVariance();
			}
			stats.setParagraphCount(paragraph_sum);
			stats.setMean(freq_sum);
			stats.setVariance(freq_squared_sum);

			// Emit the stats for the word.
			context.write(key, stats);
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class sums up each component of a word's stats, and then computes mean and variance.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
			WordStatisticsWritable stats = new WordStatisticsWritable();
			long paragraph_sum = 0L;
			double freq_sum = 0.0;
			double freq_squared_sum = 0.0;

			// Sum up the counts for the current word, specified in object "key".
			for (WordStatisticsWritable value : values) {
				paragraph_sum += value.getParagraphCount();
				freq_sum += value.getMean();
				freq_squared_sum += value.getVariance();
			}
			double mean = freq_sum / paragraph_sum;
			stats.setParagraphCount(paragraph_sum);
			stats.setMean(mean);
			stats.setVariance(freq_squared_sum / paragraph_sum - mean * mean);

			// Emit the total count for the word.
			context.write(key, stats);
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "WordStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(CombineClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}
