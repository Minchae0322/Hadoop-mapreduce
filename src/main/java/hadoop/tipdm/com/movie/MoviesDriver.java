package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;
import java.util.Map;

import static hadoop.tipdm.com.movie.Value.RATINGS_DAT_PATH;

public class MoviesDriver {

    public final static Map<Long, String> movies = new HashMap<>();
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "movie");
        job.setJarByClass(MoviesDriver.class);
        job.setMapperClass(MoviesJoinRatingsMapper.class);
        job.setCombinerClass(MovieRatingCountReducer.class);
        job.setReducerClass(MovieRatingCountReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(MovieData.class);

        FileInputFormat.addInputPath(job, new Path(RATINGS_DAT_PATH));

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}