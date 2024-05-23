package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.LongWritable;

import java.util.*;

public class Parser {

    public List<Movie> parseMovie(String[] strings) {
        List<Movie> list = new ArrayList<>();
        for (String itr : strings) {
            StringTokenizer itr2 = new StringTokenizer(itr, "::");
            Long movieId = Long.parseLong(itr2.nextToken());
            String movieName = itr2.nextToken();
            String genres = itr2.nextToken();

            Movie movie = new Movie(movieId, movieName, genres);
            list.add(movie);
        }
        return list;
    }
}
