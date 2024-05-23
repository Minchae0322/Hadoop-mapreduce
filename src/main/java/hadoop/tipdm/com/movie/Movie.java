package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Movie extends DataWritable {
    private Long movieId;

    private String movieName;

    private String genres;

    public Movie(Long movieId, String movieName, String genres) {
        this.movieId = movieId;
        this.movieName = movieName;
        this.genres = genres;
    }

    public Writable of(Movie movie) {
        return (Writable) movie;
    }

    public String getMovieName() {
        return movieName;
    }

    public Long getMovieId() {
        return movieId;
    }

    public String getGenres() {
        return genres;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(movieId);
        dataOutput.writeUTF(movieName);
        dataOutput.writeUTF(genres);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movieId = dataInput.readLong();
        movieName = dataInput.readUTF();
        genres = dataInput.readUTF();
    }

    @Override
    public int compareTo(DataWritable o) {
        return 0;
    }
}
