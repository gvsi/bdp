package com.refactorlabs.cs378.assign3;

// Imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

public class InvertedIndex extends Configured implements Tool {

    // Map and Reduce classes

	public static class MapClass extends Mapper<LongWritable, Text, Text, VerseArrayWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			int i = line.indexOf(' ');

			if (i == -1) return;

			HashSet<String> words_set = new HashSet<>();

			String verse_id = line.substring(0, i);
			String content = line.substring(i+1, line.length());
			content = content.toLowerCase()
					.replaceAll("(\\[\\d+\\])", " $1 ") // Add a space before and after [number], so it's tokenized
					.replaceAll("\\.|,|--|;|:|\\?|\"|=|_|!|\\(|\\)", " "); // Remove punctuation

			StringTokenizer tokenizer = new StringTokenizer(content);

			while (tokenizer.hasMoreTokens()) {
				words_set.add(tokenizer.nextToken());
			}

			String[] verse_arr = {verse_id};
			VerseArrayWritable verse = new VerseArrayWritable(verse_arr);

			for (String s : words_set) {
				word.set(s);
				if (s.equals("perverseness")) {
					System.out.println("Writing " + word.toString() + " -> " + verse);
				}
				context.write(word, verse);
			}
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, VerseArrayWritable, Text, VerseArrayWritable> {

		@Override
		public void reduce(Text key, Iterable<VerseArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> verses = new ArrayList<>();

			for (VerseArrayWritable value : values) {
				for (String s : value.getVerses()) {
					verses.add(s);
				}
			}
			String[] verses_arr = new String[verses.size()];
			verses.toArray(verses_arr);
			VerseArrayWritable res = new VerseArrayWritable(verses_arr);
			res.sort();

			context.write(key, res);
		}
	}

    /**
     * The run method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	Job job = Job.getInstance(conf, "InvertedIndex");
	// Identify the JAR file to replicate to all machines.
	job.setJarByClass(InvertedIndex.class);

	// Set the output key and value types (for map and reduce).
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(VerseArrayWritable.class);

	// Set the map and reduce classes (and combiner, if used).
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
	int res = ToolRunner.run(new InvertedIndex(), args);
	System.exit(res);
    }
}
