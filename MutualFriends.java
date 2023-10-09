
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {

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

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //initialize set and stringBuilder
            HashSet<String> set = new HashSet<>();
            StringBuilder sb = new StringBuilder();

            for (Text friends : values)
            {
                //split on the comma
                List<String> inputFriends = Arrays.asList(friends.toString().split(","));
                for (String findFriend : inputFriends)
                {
                    // If there's a duplicate value in the map, that's a mutual friend
                    if(set.contains(findFriend)){
                        sb.append(findFriend + ',');
                    }
                    // If seeing a value for the first time, add to set
                    else {
                        set.add(findFriend);
                    }
                }
            }
            if (sb.lastIndexOf(",") > -1){
                sb.deleteCharAt(sb.lastIndexOf(","));
            }

            //make it strings and write to output file
            result.set(new Text(sb.toString()));

            //Q1 Answer: Find these pairs
            if(key.toString().equals("0,1") || key.toString().equals("20,28193") ||
                    key.toString().equals("1,29826") || key.toString().equals("6222,19272") || key.toString().equals("28056,28091")){
                //print these answers and write to output file
                System.out.println(key + "\t" + result);
                context.write(key, result);
            }
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriend <file input path> <file output path>");
            System.err.println(otherArgs[0]);
            System.exit(2);
        }


        Job job = new Job(conf, "mutualfriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

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