package cs435.pa2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  Composite Key class: https://stackoverflow.com/questions/12427090/hadoop-composite-key
 *
 *  Used for combining docId and a unigram
 * */
public class CompositeKey implements WritableComparable<CompositeKey> {

    public Text docId;
    public Text unigram;

    public CompositeKey() {
        this.docId = new Text();
        this.unigram = new Text();
    }

    public CompositeKey(String docId, String unigram) {
        this.docId = new Text(docId);
        this.unigram = new Text(unigram);
    }

    public void write(DataOutput out) throws IOException {
        this.docId.write(out);
        this.unigram.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.docId.readFields(in);
        this.unigram.readFields(in);
    }

    public int compareTo(CompositeKey other) {
        int docIdCompare = this.docId.toString().compareTo(other.docId.toString());
        if (docIdCompare == 0) {
            return this.unigram.toString().compareTo(other.unigram.toString());
        }
        return docIdCompare;
    }

    @Override
    public String toString() {
        return this.docId.toString() + "\t" + this.unigram.toString();
    }
}
