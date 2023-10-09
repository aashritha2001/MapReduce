
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InMemoryJoin {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{

        private HashMap<String, Integer> ageMap = new HashMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //split on tab each line
            String[] input = value.toString().split("\t");

            if(input.length == 2){
                String user = input[0];
                String[] friends = input[1].split(",");
                for(String friend2: friends){
                    // for each ID, get the direct friend's age from map and write
                    context.write(new Text(user), new Text(ageMap.get(friend2).toString()));
                }
            }
        }


        @Override
        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    if (cacheFile.toString().endsWith("userdata.txt")) {
                        // Read and store "userdata.txt" in the userDataMap
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
                        LocalDate referenceDate = LocalDate.parse("01/01/2022", formatter);
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFile))))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                // read contents of file
                                String[] parts = line.split(",");

                                // get user's birthdate
                                LocalDate birthdate = LocalDate.parse(parts[9], formatter);

                                // Calculate the difference in years between birthdate and reference date
                                Period period = Period.between(birthdate, referenceDate);
                                int age = period.getYears();

                                //add userID and age to map
                                if(!ageMap.containsKey(parts[0])){
                                    ageMap.put(parts[0], age);
                                }
                            }
                        }
                    }
                }
            }

        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int ageSum = 0;
            int count = 0;

            // for all the same key, add up the ages
            for(Text age: values){
                ageSum += Integer.parseInt(age.toString());
                count++;
            }

            // average the ages to get the direct friends average age
            double avg = (double)ageSum/(double)count;

            // write results to output file
            result.set(new Text(Double.toString(avg)));
            context.write(key, result);
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Input order: mutual friends data path, output path");
            System.err.println(otherArgs[1]);
            System.exit(1);
        }


        Job job = new Job(conf, "InMemoryJoin");
        job.setJarByClass(InMemoryJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

//        // Import the second file to cache memory
        job.addCacheFile(new Path(otherArgs[1]).toUri());

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}