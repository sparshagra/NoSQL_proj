package nosql.pig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LFLineLoader extends LoadFunc {
    private org.apache.hadoop.mapreduce.RecordReader reader;
    private long lineNumber = 0;

    public static class LFTextInputFormat extends TextInputFormat {
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        job.getConfiguration().set(
            "textinputformat.record.delimiter", "\n"
        );
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() {
        return new LFTextInputFormat();
    }

    @Override
    public void prepareToRead(
        org.apache.hadoop.mapreduce.RecordReader reader,
        PigSplit split
    ) {
        this.reader = reader;
        this.lineNumber = 0;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }

            Text value = (Text) reader.getCurrentValue();
            byte[] bytes = value.getBytes();
            int length = value.getLength();

            if (length > 0 && bytes[length - 1] == 0x0D) {
                length--;
            }

            String line = new String(
                bytes, 0, length, StandardCharsets.ISO_8859_1
            );

            lineNumber++;

            Tuple tuple = TupleFactory.getInstance().newTuple(2);
            tuple.set(0, lineNumber);
            tuple.set(1, line);
            return tuple;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading input", e);
        }
    }
}
