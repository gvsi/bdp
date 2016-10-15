package com.refactorlabs.cs378.assign7;

/**
 * Created by gvsi on 10/14/16.
 */
// Imports

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UserSessionsFiltering extends Configured implements Tool {

    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        private Random random = new Random();

        private AvroMultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new AvroMultipleOutputs(context);
        }

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            Session session = value.datum();

            List<Event> events = session.getEvents();

            // Filter out sessions with more than 100 events
            if (events.size() > 100) {
                context.getCounter(MAPPER_COUNTER_GROUP, "Large Sessions Discarded").increment(1L);
                return;
            }

            boolean hasChangeContactFormEvent = false;
            boolean hasEditContactFormEvent = false;
            boolean hasClickEvent = false;
            boolean hasShowEvent = false;
            boolean hasDisplayEvent = false;
            boolean hasVisitEvent = false;

            // Loop through the events to get their types;
            for (Event e : events) {
                switch (e.getEventType()) {
                    case CHANGE:
                        if (e.getEventSubtype() == EventSubtype.CONTACT_FORM)
                            hasChangeContactFormEvent = true;
                        break;
                    case CLICK:
                        hasClickEvent = true;
                        break;
                    case DISPLAY:
                        hasDisplayEvent = true;
                        break;
                    case EDIT:
                        if (e.getEventSubtype() == EventSubtype.CONTACT_FORM)
                            hasEditContactFormEvent = true;
                        break;
                    case SHOW:
                        hasShowEvent = true;
                        break;
                    case VISIT:
                        hasVisitEvent = true;
                        break;
                    default:
                        break;
                }

            }

            SessionType sessionCategory;

            // Determine session category
            if (hasChangeContactFormEvent || hasEditContactFormEvent) {
                sessionCategory = SessionType.SUBMITTER;
            } else if (hasClickEvent) {
                sessionCategory = SessionType.CLICKER;
            } else if (hasShowEvent || hasDisplayEvent) {
                sessionCategory = SessionType.SHOWER;
            } else if (hasVisitEvent) {
                sessionCategory = SessionType.VISITOR;
            } else {
                sessionCategory = SessionType.OTHER;
            }

            if (sessionCategory == SessionType.CLICKER) {
                // generate random integer between 1 and 10
                int randomInt = random.nextInt(10) + 1;

                if (randomInt == 1) {
                    multipleOutputs.write(SessionType.CLICKER.getText(), key, value);
                } else {
                    context.getCounter(MAPPER_COUNTER_GROUP, "CLICKER Session Discarded").increment(1L);
                }
            } else if (sessionCategory == SessionType.SHOWER) {
                // generate random integer between 1 and 50
                int randomInt = random.nextInt(50) + 1;

                if (randomInt == 1) {
                    multipleOutputs.write(SessionType.SHOWER.getText(), key, value);
                } else {
                    context.getCounter(MAPPER_COUNTER_GROUP, "SHOWER Session Discarded").increment(1L);
                }
            } else {
                multipleOutputs.write(sessionCategory.getText(), key, value);
            }
        }

        public void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
                throws InterruptedException, IOException{
            multipleOutputs.close();
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "UserSessionsFiltering");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessionsFiltering.class);

        AvroMultipleOutputs.setCountersEnabled(job, true);

        // Set up the map
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        job.setMapperClass(MapClass.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Set up input and output
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());

        MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, Text.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Add named outputs
        AvroMultipleOutputs.addNamedOutput(job, SessionType.SUBMITTER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        AvroMultipleOutputs.addNamedOutput(job, SessionType.CLICKER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        AvroMultipleOutputs.addNamedOutput(job, SessionType.SHOWER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        AvroMultipleOutputs.addNamedOutput(job, SessionType.VISITOR.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        AvroMultipleOutputs.addNamedOutput(job, SessionType.OTHER.getText(), AvroKeyValueOutputFormat.class,
                Schema.create(Schema.Type.STRING), Session.getClassSchema());

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new UserSessionsFiltering(), args);
        System.exit(res);
    }
}
