package hadoop.tipdm.com.movie;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;


public class MoviesJoinRatingsMapper
        extends Mapper<Object, Text, LongWritable, MovieData> {

    private static Map<Long, String> movies = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        movies = readFile();
    }

    private Map<Long, String> readFile() throws IOException {
        Map<Long, String> movies = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("/usr/local/movies.dat"))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line, "::");
                Long movieId = Long.parseLong(itr.nextToken());
               //String movieName = itr.nextToken();
                String genres = itr.nextToken();
                if (genres.equals("") || genres == null) {
                    movies.put(movieId, "not");
                }
                movies.put(movieId, genres);
            }
        }

        return movies;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] itrr = value.toString().split("\\n");

        for (String itr : itrr) {
            StringTokenizer itr2 = new StringTokenizer(itr, "::");
            Long userId = Long.parseLong(itr2.nextToken());
            Long movieId = Long.parseLong(itr2.nextToken());
            int rating = Integer.parseInt(itr2.nextToken());
            Long timestamp = Long.parseLong(itr2.nextToken());

            //context.write(new LongWritable(movieId), new MovieData(userId + "::" + movieId + "::" + rating + "::" + timestamp + "::" + movies.get(movieId)));
            context.write(new LongWritable(movieId), new MovieData(movieId, movies.get(movieId), userId, rating, timestamp));
        }

    }
}
/*
public class MoviesJoinRatingsMapper
        extends Mapper<Object, Text, LongWritable, Text> {

    private static Map<Long, String> movies = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        movies = readFile();
    }

    private Map<Long, String> readFile() throws IOException {
        Map<Long, String> movies = new HashMap<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("/usr/local/movies.dat"))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                StringTokenizer itr = new StringTokenizer(line, "::");
                Long movieId = Long.parseLong(itr.nextToken());
                String movieName = itr.nextToken();
                String genres = itr.nextToken();
                if (genres.equals("") || genres == null) {
                    movies.put(movieId, "not");
                }
                movies.put(movieId, genres);
            }
        }

        return movies;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] itrr = value.toString().split("\\n");

        for (String itr : itrr) {
            StringTokenizer itr2 = new StringTokenizer(itr, "::");
            Long userId = Long.parseLong(itr2.nextToken());
            Long movieId = Long.parseLong(itr2.nextToken());
            int rating = Integer.parseInt(itr2.nextToken());
            Long timestamp = Long.parseLong(itr2.nextToken());

            MovieRate movieRate = new MovieRate(userId, movieId, rating, timestamp);

            //context.write(new LongWritable(movieId), new MovieData(userId + "::" + movieId + "::" + rating + "::" + timestamp + "::" + movies.get(movieId)));
            context.write(new LongWritable(movieId), new MovieData(movieId, "movie", movies.get(movieId), userId, rating, timestamp));
        }

    }
}*/
