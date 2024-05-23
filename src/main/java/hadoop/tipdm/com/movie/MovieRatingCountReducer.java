package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class MovieRatingCountReducer extends Reducer<LongWritable, MovieData, LongWritable, MovieData> {

    protected void reduce(LongWritable key, Iterable<MovieData> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        String genres = "";
        for (MovieData value : values) {
            count++;
            if (genres.equals("")) {
                genres = value.toString();
            }
           /* if (genres.equals("")) {
                String[] str = value.toString().split("::");
                genres = str[2];
            }*/
        }
        context.write(new LongWritable(key.get()), new MovieData(genres, count));
    }
}
