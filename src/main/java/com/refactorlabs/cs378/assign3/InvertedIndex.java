package com.refactorlabs.cs378.assign3;

// Imports

public class InvertedIndex extends Configured implements Tool {

    // Map and Reduce classes

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

	// Set the map and reduce classes (and combiner, if used).

	// Set the input and output file formats.

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
