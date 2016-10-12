package com.refactorlabs.cs378.assign6;

// Imports

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class UserSessionsJoin extends Configured implements Tool {

	public static class SessionMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

		private Text word = new Text();

		@Override
		public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
				throws IOException, InterruptedException {
            // String userId = key.datum().toString();
            Session session = value.datum();

            HashSet<String> uniqueVins = new HashSet<>();
            HashSet<String> uniqueContactFormVins = new HashSet<>();
            HashMap<String, HashMap<CharSequence, Long>> vinClickEvents = new HashMap<>();

            for (Event e : session.getEvents()) {
                String vin = e.getVin().toString();

                uniqueVins.add(vin);

                if (e.getEventType() == EventType.EDIT && e.getEventSubtype() == EventSubtype.CONTACT_FORM) {
                    uniqueContactFormVins.add(vin);
                }

                if (e.getEventType() == EventType.CLICK) {
                    if (vinClickEvents.containsKey(vin)) {
                        vinClickEvents.get(vin).put(e.getEventSubtype().name(), 1L);
                    } else {
                        HashMap<CharSequence, Long> clickEvents = new HashMap<>();
                        clickEvents.put(e.getEventSubtype().name(), 1L);
                        vinClickEvents.put(vin, clickEvents);
                    }
                }
            }

            VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();

            for (String vin : uniqueVins) {
                vinImpressionBuilder.setUniqueUsers(1L);

                // reset field first
                vinImpressionBuilder.setEditContactForm(0);
                if (uniqueContactFormVins.contains(vin)) {
                    vinImpressionBuilder.setEditContactForm(1L);
                }

                if (vinClickEvents.containsKey(vin)) {
                    vinImpressionBuilder.setClicks(vinClickEvents.get(vin));
                }

                word.set(vin);
                context.write(word, new AvroValue(vinImpressionBuilder.build()));
            }
        }
	}

    public static class VinImpressionMapClass extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] vinImpressionSplit = value.toString().split(",");
            String vin = vinImpressionSplit[0];
            String impression_type = vinImpressionSplit[1];
            Long count = Long.parseLong(vinImpressionSplit[2]);



            VinImpressionCounts.Builder vinImpressionBuilder = VinImpressionCounts.newBuilder();
            if (impression_type.equals("SRP")) {
                vinImpressionBuilder.setMarketplaceSrps(count);
            } else if (impression_type.equals("VDP")) {
                vinImpressionBuilder.setMarketplaceVdps(count);
            }
            word.set(vin);

            context.write(word, new AvroValue<VinImpressionCounts>(vinImpressionBuilder.build()));

        }
    }

    // Static method shared by combiner and reducer to combine all impression counts with the same vin
    public static VinImpressionCounts.Builder combineImpressionCounts(Text key, Iterable<AvroValue<VinImpressionCounts>> values) {
        VinImpressionCounts.Builder builder = VinImpressionCounts.newBuilder();

        // Map storing all clickEvents
        HashMap<CharSequence, Long> finalClickEvents = new HashMap<CharSequence, Long>();


        for (AvroValue<VinImpressionCounts> value : values) {
            // context.write(key, value);


            VinImpressionCounts vinImpression = value.datum();

            // Check whether the data is coming from vim impressions or user sessions
            if (vinImpression.getMarketplaceSrps() > 0 || vinImpression.getMarketplaceVdps() > 0) {
                if (vinImpression.getMarketplaceSrps() > 0) {
                    builder.setMarketplaceSrps(vinImpression.getMarketplaceSrps());
                }
                if (vinImpression.getMarketplaceVdps() > 0) {
                    builder.setMarketplaceVdps(vinImpression.getMarketplaceVdps());
                }
            } else {
                builder.setUniqueUsers(builder.getUniqueUsers() + vinImpression.getUniqueUsers());
                builder.setEditContactForm(builder.getEditContactForm() + vinImpression.getEditContactForm());

                Map<CharSequence, Long> clicks = vinImpression.getClicks();
                if (clicks != null) {
                    for (CharSequence eventSubtype : clicks.keySet()) {
                        if (finalClickEvents.containsKey(eventSubtype)) {
                            finalClickEvents.put(eventSubtype, finalClickEvents.get(eventSubtype) + clicks.get(eventSubtype));
                        } else {
                            finalClickEvents.put(eventSubtype, clicks.get(eventSubtype));
                        }
                    }
                }
            }
        }

        builder.setClicks(finalClickEvents);
        return builder;
    }

    public static class CombineClass extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder builder = combineImpressionCounts(key, values);

            context.write(key, new AvroValue<VinImpressionCounts>(builder.build()));
        }
    }

	public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

		@Override
		public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
				throws IOException, InterruptedException {

            VinImpressionCounts.Builder builder = combineImpressionCounts(key, values);

            // Filter out users without data in user sessions (left join)
            if (builder.getUniqueUsers() > 0)
                context.write(key, new AvroValue<VinImpressionCounts>(builder.build()));
		}
	}

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "UserSessionsJoin");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessionsJoin.class);

        MultipleInputs.addInputPath(job, new Path(appArgs[0]), AvroKeyValueInputFormat.class, SessionMapClass.class);
        MultipleInputs.addInputPath(job, new Path(appArgs[1]), TextInputFormat.class, VinImpressionMapClass.class);

        // Configure mapper
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());

        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Configure reducer
        job.setReducerClass(ReduceClass.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Configure combiner
        job.setCombinerClass(CombineClass.class);

        // Grab the input file and output directory from the command line.
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new UserSessionsJoin(), args);
        System.exit(res);
    }
}
