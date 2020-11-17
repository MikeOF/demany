package demany.DataFlow;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class CompressedSequenceGroup {

    public final HashMap<String, ByteArrayOutputStream> byteArrayByReadType = new HashMap<>();
    public final HashMap<String, GZIPOutputStream> gzipOutputStreamByReadType = new HashMap<>();
    public final HashMap<String, BufferedWriter> bufferedWriterByReadType = new HashMap<>();
    private boolean completed = false;
    private boolean sequencesWritten = false;

    public CompressedSequenceGroup(Set<String> readTypeSet) throws IOException {

        for (String readType : readTypeSet) {

            // create the byte array and buffered writer
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(32768);
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(gzipOutputStream));

            // store the byte array output stream
            this.byteArrayByReadType.put(readType, byteArrayOutputStream);

            // store the gzip output stream
            this.gzipOutputStreamByReadType.put(readType, gzipOutputStream);

            // store the buffered writer
            this.bufferedWriterByReadType.put(readType, bufferedWriter);
        }
    }

    public void addSequence(String readType, SequenceLines sequenceLines) throws IOException {

        if (this.completed) { throw new RuntimeException("cannot add sequence lines to a completed sequence group"); }

        BufferedWriter writer = this.bufferedWriterByReadType.get(readType);

        writer.write(sequenceLines.line1);
        writer.newLine();

        writer.write(sequenceLines.line2);
        writer.newLine();

        writer.write(sequenceLines.line3);
        writer.newLine();

        writer.write(sequenceLines.line4);
        writer.newLine();

        this.sequencesWritten = true;
    }

    public void markCompleted() throws IOException {

        if (this.completed) { throw new RuntimeException("a sequence group should not be marked completed twice"); }

        // flush all the writers
        for (BufferedWriter writer : this.bufferedWriterByReadType.values()) { writer.flush(); }

        // finish all the gzip output streams
        for (GZIPOutputStream gzipOutputStream : this.gzipOutputStreamByReadType.values()) {
            gzipOutputStream.finish();
        }

        this.completed = true;
    }

    public boolean isCompleted() { return this.completed; }

    public boolean isEmpty() { return !this.sequencesWritten; }
}
