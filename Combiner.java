
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Combiner {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // get the userID and user input from userdata.txt
            int userID = Integer.parseInt(value.toString().substring(0, value.toString().indexOf(',')));
            String[] input = value.toString().substring(value.toString().indexOf(',')+1).split(",");

            // make the userdata the key and the userID the value
            for(String userVal: input){
                context.write(new Text(userVal), new Text(Integer.toString(userID)));
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder("[");

            // add each userID where the key occurs to string builder
            for(Text value: values){
                sb.append(value.toString() + ",");
            }

            if (sb.lastIndexOf(",") > -1){
                sb.deleteCharAt(sb.lastIndexOf(","));
            }

            sb.append("]");

            //make it strings and write to output file
            result.set(new Text(sb.toString()));
            context.write(key, result);

        }
    }

    public static class InverseCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private HashMap<String, Integer> wordCounts = new HashMap<>();
        private int maxOccurrences = 0;

        // term cannot contain any numbers or dates
        public boolean isValid(String term){
            return term.matches("^[a-zA-Z]*$");
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String term = key.toString();

            if(isValid(term)){

                int count = 0;

                for(Text value: values){
                    count++;
                }

                wordCounts.put(term, count);

                if (count > maxOccurrences) {
                    maxOccurrences = count;
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            // Iterate through the wordCounts to find words with maximum occurrences
            for (String word : wordCounts.keySet()) {
                int count = wordCounts.get(word);

                if (count == maxOccurrences) {
                    result.set(Integer.toString(count));
                    context.write(new Text(word), result);
                }
            }
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Input order: data path, output path");
            System.err.println(otherArgs[1]);
            System.exit(1);
        }

        Job job = new Job(conf, "InvertedIndex");
        job.setJarByClass(Combiner.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(InverseCombiner.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}