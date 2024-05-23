package hadoop.tipdm.com.movie;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class DataWritable implements WritableComparable<DataWritable> {
}
