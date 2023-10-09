
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ReduceJoin {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
        private Text friendPair = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //split on tab each line
            String[] input = value.toString().split("\t");

            if(input.length == 2){
                String user = input[0];
                String[] friends = input[1].split(",");
                for(String friend2: friends){
                    if(Integer.parseInt(user) < Integer.parseInt(friend2)){
                        friendPair.set(user + "," + friend2);
                    }
                    else{
                        friendPair.set(friend2 + "," + user);
                    }
                    context.write(friendPair, new Text(input[1]));
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();
        private HashMap<String, String> dobMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    if (cacheFile.toString().endsWith("userdata.txt")) {

                        FileSystem fs = FileSystem.get(context.getConfiguration());

                        // Read and store "userdata.txt" in the userDataMap
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFile))))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                // read contents of file
                                String[] parts = line.split(",");

                                // add all the userIDs ad dobs to the hashmap
                                if(!dobMap.containsKey(parts[0])){
                                    dobMap.put(parts[0], parts[9]);
                                }
                            }
                        }
                    }
                }
            }
        }

        // given a date of birth, this function calculates age in relation to the reference date 01/01/2022
        // This function also checks that the individual if born after 12/21/1981 and if not, returns -1
        public int calculateAge(String dob){
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
            LocalDate referenceDate = LocalDate.parse("01/01/2023", formatter);

            LocalDate DOB = LocalDate.parse(dob, formatter);
            LocalDate cutoffDate = LocalDate.of(1981, 12, 31);

            //Calculate the difference in years between birthdate and reference date
            Period period = Period.between(DOB, referenceDate);

            if(DOB.isAfter(cutoffDate)){
                return period.getYears();
            }
            return -1;
        }


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //initialize set and stringBuilder
            HashSet<String> set = new HashSet<>();
            StringBuilder sb = new StringBuilder();
            sb.append('[');

            List<Integer> ages = new ArrayList<>();

            for (Text friends : values)
            {
                //split on the comma
                List<String> inputFriends = Arrays.asList(friends.toString().split(","));
                for (String findFriend : inputFriends)
                {
                    // If there's a duplicate value in the map, that's a mutual friend
                    if(set.contains(findFriend)){
                        sb.append(dobMap.get(findFriend) + ',');
                        int validAge = calculateAge(dobMap.get(findFriend));
                        if(validAge != -1){
                            ages.add(validAge);
                        }
                    }
                    // If seeing a value for the first time, add to set
                    else {
                        set.add(findFriend);
                    }
                }
            }

            //removes last comma
            if (sb.lastIndexOf(",") > -1){
                sb.deleteCharAt(sb.lastIndexOf(","));
            }

            int maxAge = 0;
            // If there's only one age in the age list, make that max age
            if(ages.size() > 0){
                maxAge = ages.get(0);
            }

            // If there's more than 1 age in the age list, find the max
            if(ages.size() > 1){
                // Iterate through the ages list to get the maximum value
                for (int i = 1; i < ages.size(); i++) {
                    int currentNumber = ages.get(i);
                    if (currentNumber > maxAge) {
                        maxAge = currentNumber;
                    }
                }
            }

            // add the closing square bracket and write to output file
            sb.append("], " + maxAge);
            result.set(new Text(sb.toString()));
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


        Job job = new Job(conf, "ReduceJoin");
        job.setJarByClass(ReduceJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // Import the second file to cache memory
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