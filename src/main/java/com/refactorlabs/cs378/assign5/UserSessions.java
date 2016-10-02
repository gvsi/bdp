package com.refactorlabs.cs378.assign5;

// Imports

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UserSessions extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, AvroKey<CharSequence>, AvroValue<Session>> {

		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

            String[] fields = line.split("\\t");

            // Set all empty fields to null
            for (int i = 0; i < fields.length; i++) {
                if (fields[i].isEmpty()) {
                    fields[i] = null;
                }
            }

            String user_id = fields[0];

            Session.Builder sessionBuilder = Session.newBuilder();
            sessionBuilder.setUserId(user_id);

            Event.Builder eventBuilder = Event.newBuilder();

            /*
                Building the event here
             */

            // Split event type and subtype
            String fullType = fields[1];
            int s = fullType.indexOf(' ');
            String eventType = fullType.substring(0, s);
            String eventSubtype = fullType.substring(s + 1, fullType.length());

            EventType e_t;
            switch (eventType) {
                case "change":
                    e_t = EventType.CHANGE;
                    break;
                case "click":
                    e_t = EventType.CLICK;
                    break;
                case "display":
                    e_t = EventType.DISPLAY;
                    break;
                case "edit":
                    e_t = EventType.EDIT;
                    break;
                case "show":
                    e_t = EventType.SHOW;
                    break;
                case "visit":
                    e_t = EventType.VISIT;
                    break;
                default:
                    e_t = null;
                    break;
            }
            eventBuilder.setEventType(e_t);

            EventSubtype e_st;
            switch (eventSubtype) {
                case "alternative":
                    e_st = EventSubtype.ALTERNATIVE;
                    break;
                case "badge detail":
                    e_st = EventSubtype.BADGE_DETAIL;
                    break;
                case "badges":
                    e_st = EventSubtype.BADGES;
                    break;
                case "contact button":
                    e_st = EventSubtype.CONTACT_BUTTON;
                    break;
                case "contact form":
                    e_st = EventSubtype.CONTACT_FORM;
                    break;
                case "features":
                    e_st = EventSubtype.FEATURES;
                    break;
                case "get directions":
                    e_st = EventSubtype.GET_DIRECTIONS;
                    break;
                case "market report":
                    e_st = EventSubtype.MARKET_REPORT;
                    break;
                case "photo modal":
                    e_st = EventSubtype.PHOTO_MODAL;
                    break;
                case "vehicle history":
                    e_st = EventSubtype.VEHICLE_HISTORY;
                    break;
                default:
                    e_st = null;
                    break;
            }
            eventBuilder.setEventSubtype(e_st);

            eventBuilder.setPage(fields[2]);

            // Set referring domain
            String referringDomain = null;
            if (!fields[3].equals("null") && !fields[3].isEmpty()) {
                referringDomain = fields[3];
            }
            eventBuilder.setReferringDomain(referringDomain);


            eventBuilder.setEventTime(fields[4]);
            eventBuilder.setCity(fields[5]);
            eventBuilder.setVin(fields[6]);

            // Set vehicle condition
            VehicleCondition vehicleCondition;
            switch (fields[7]) {
                case "New":
                    vehicleCondition = VehicleCondition.NEW;
                    break;
                case "Used":
                    vehicleCondition = VehicleCondition.USED;
                    break;
                default:
                    vehicleCondition = null;
                    break;
            }
            eventBuilder.setCondition(vehicleCondition);


            eventBuilder.setYear(Integer.parseInt(fields[8]));
            eventBuilder.setMake(fields[9]);
            eventBuilder.setModel(fields[10]);
            eventBuilder.setTrim(fields[11]);


            // Set body style
            BodyStyle bodyStyle;

            switch (fields[12]) {
                case "Convertible":
                    bodyStyle = BodyStyle.CONVERTIBLE;
                    break;
                case "Couple":
                    bodyStyle = BodyStyle.COUPLE;
                    break;
                case "Hatchback":
                    bodyStyle = BodyStyle.HATCHBACK;
                    break;
                case "Minivan":
                    bodyStyle = BodyStyle.MINIVAN;
                    break;
                case "Pickup":
                    bodyStyle = BodyStyle.PICKUP;
                    break;
                case "SUV":
                    bodyStyle = BodyStyle.SUV;
                    break;
                case "Sedan":
                    bodyStyle = BodyStyle.SEDAN;
                    break;
                case "Van":
                    bodyStyle = BodyStyle.VAN;
                    break;
                case "Wagon":
                    bodyStyle = BodyStyle.WAGON;
                    break;
                default:
                    bodyStyle = null;
                    break;
            }
            eventBuilder.setBodyStyle(bodyStyle);

            // Set cab style
            CabStyle cabStyle;
            switch (fields[13]) {
                case "Crew Cab":
                    cabStyle = CabStyle.CREW_CAB;
                    break;
                case "Extended Cab":
                    cabStyle = CabStyle.EXTENDED_CAB;
                    break;
                case "Regular Cab":
                    cabStyle = CabStyle.REGULAR_CAB;
                    break;
                default:
                    cabStyle = null;
                    break;
            }
            eventBuilder.setCabStyle(cabStyle);


            eventBuilder.setPrice(Float.parseFloat(fields[14]));
            eventBuilder.setMileage(Long.parseLong(fields[15]));
            eventBuilder.setImageCount(Long.parseLong(fields[16]));


            // Set carfax free report
            Boolean carfax_free_report;
            switch (fields[17]) {
                case "t":
                    carfax_free_report = true;
                    break;
                case "f":
                    carfax_free_report = false;
                    break;
                default:
                    carfax_free_report = null;
                    break;
            }
            eventBuilder.setCarfaxFreeReport(carfax_free_report);

            // Set features
            CharSequence[] features = fields[18].split(":");
            if (!features[0].equals("null")) {
                Arrays.sort(features);
                eventBuilder.setFeatures(Arrays.asList(features));
            } else {
                eventBuilder.setFeatures(null);
            }


            AvroValue<Event> event = new AvroValue<Event>(eventBuilder.build());
            ArrayList<Event> event_list = new ArrayList<>();
            event_list.add(event.datum());

            sessionBuilder.setEvents(event_list);

            context.write(new AvroKey<CharSequence>(user_id), new AvroValue<Session>(sessionBuilder.build()));
		}
	}

	/**
	 * The Reduce class also used as a Combiner. It gets an array Verse location, sorts them and emits them.
	 */
	public static class ReduceClass extends Reducer<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

		@Override
		public void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<Session>> values, Context context)
				throws IOException, InterruptedException {

            HashSet<Event> events_set = new HashSet<>();

			for (AvroValue<Session> value : values) {
                events_set.addAll(value.datum().getEvents());
			}

			ArrayList<Event> events = new ArrayList<>(events_set);

            // Sort by date, then event type
            Collections.sort(events, new Comparator<Event>() {
                @Override
                public int compare(Event e1, Event e2) {
                    String d1 = e1.getEventTime().toString();
                    String d2 = e2.getEventTime().toString();
                    if (!d1.equals(d2)) {
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
                        try {
                            Date d_d1 = formatter.parse(d1);
                            Date d_d2 = formatter.parse(d2);
                            if (d_d1.after(d_d2)) {
                                return 1;
                            } else {
                                return -1;
                            }
                        } catch (ParseException e) {
                            System.out.println("Exception "+e);
                        }
                    } else {
                        return e1.getEventType().compareTo(e2.getEventType());
                    }
                    return 0;
                }
            });
            String user_id = key.toString();

            Session.Builder sessionBuilder = Session.newBuilder();
            sessionBuilder.setUserId(user_id);
            sessionBuilder.setEvents(events);

			context.write(key, new AvroValue<Session>(sessionBuilder.build()));
		}
	}

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "UserSessions");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessions.class);

        // Set the output key and value types (for map and reduce).
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Set the map and reduce classes and combiner.
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
        int res = ToolRunner.run(new UserSessions(), args);
        System.exit(res);
    }
}
