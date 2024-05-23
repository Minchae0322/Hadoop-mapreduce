package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static io.netty.util.internal.EmptyArrays.EMPTY_BYTES;
import static org.apache.hadoop.io.Text.decode;
import static org.apache.hadoop.io.Text.encode;

public class MovieData implements WritableComparable<MovieData> {

    private byte[] bytes;

    private int length;

    private Long movieId;


    private String genres;

    private  Long userId;

    private Integer rating;

    private Long timestamp;

    public MovieData() {
        this.bytes = EMPTY_BYTES;
    }

    public MovieData(String genres) {
        this.genres = genres;
        set(genres);
    }

    public MovieData(Long movieId, String genres, Long userId, Integer rating, Long timestamp) {
        this.movieId = movieId;
        this.genres = genres;
        this.userId = userId;
        this.rating = rating;
        this.timestamp = timestamp;
        set(userId + "::" + movieId + "::" + rating + "::" + timestamp + "::" + timestamp + "::" + genres);
    }


    private void set(String string) {
        try {
            ByteBuffer bb = encode(string, true);
            this.bytes = bb.array();
            this.length = bb.limit();
        } catch (CharacterCodingException var3) {
            throw new RuntimeException("Should not have happened ", var3);
        }
    }

    public MovieData(String genres, Integer count) {
        set(genres + "  " + count);

    }



    @Override
    public int compareTo(MovieData o) {
        return 0;
    }

    public String toString() {
        try {
            return decode(this.bytes, 0, this.length);
        } catch (CharacterCodingException var2) {
            throw new RuntimeException("Should not have happened ", var2);
        }
    }



    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int newLength = WritableUtils.readVInt(in);
        this.readWithKnownLength(in, newLength);
    }

    public void readWithKnownLength(DataInput in, int len) throws IOException {
        this.setCapacity(len, false);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    private void setCapacity(int len, boolean keepData) {
        if (this.bytes == null || this.bytes.length < len) {
            if (this.bytes != null && keepData) {
                this.bytes = Arrays.copyOf(this.bytes, Math.max(len, this.length << 1));
            } else {
                this.bytes = new byte[len];
            }
        }

    }

    public byte[] getBytes() {
        return bytes;
    }

    public Long getMovieId() {
        return movieId;
    }


    public String getGenres() {
        return genres;
    }

    public Long getUserId() {
        return userId;
    }

    public Integer getRating() {
        return rating;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
