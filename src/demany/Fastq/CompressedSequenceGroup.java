package demany.Fastq;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class CompressedSequenceGroup {

    private static class BufferedWriterCountPair {

        final BufferedWriter bufferedWriter;
        int count = 0;

        BufferedWriterCountPair(BufferedWriter bufferedWriter) { this.bufferedWriter = bufferedWriter; }
    }

    public final HashMap<String, ByteArrayOutputStream> byteArrayByReadType = new HashMap<>();
    private final HashMap<String, GZIPOutputStream> gzipOutputStreamByReadType = new HashMap<>();
    private final HashMap<String, BufferedWriterCountPair> bufferedWriterCountByReadType = new HashMap<>();
    private boolean completed = false;
    private int sequencesWritten = 0;

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
            this.bufferedWriterCountByReadType.put(readType, new BufferedWriterCountPair(bufferedWriter));
        }
    }

    public void addSequence(String readType, SequenceLines sequenceLines) throws IOException {

        if (this.completed) { throw new RuntimeException("cannot add sequence lines to a completed sequence group"); }

        BufferedWriterCountPair pair = this.bufferedWriterCountByReadType.get(readType);
        BufferedWriter writer = pair.bufferedWriter;

        writer.write(sequenceLines.line1); writer.newLine();

        writer.write(sequenceLines.line2); writer.newLine();

        writer.write(sequenceLines.line3); writer.newLine();

        writer.write(sequenceLines.line4); writer.newLine();

        pair.count++;
    }

    public void markCompleted() throws IOException {

        if (this.completed) { throw new RuntimeException("a sequence group should not be marked completed twice"); }

        // flush all the writers and check the number of sequences written to each
        int sequencesWritten = -1;
        for (BufferedWriterCountPair pair : this.bufferedWriterCountByReadType.values()) {

            // check the number of sequences written
            if (sequencesWritten == -1) {
                sequencesWritten = pair.count;

            } else if (sequencesWritten != pair.count) {

                throw new RuntimeException(
                        "a compressed sequence group with different number of sequences written cannot be completed"
                );
            }

            // flush the writer
            pair.bufferedWriter.flush();
        }

        // finish all the gzip output streams
        for (GZIPOutputStream gzipOutputStream : this.gzipOutputStreamByReadType.values()) {
            gzipOutputStream.finish();
        }

        this.completed = true;
        this.sequencesWritten = sequencesWritten;
    }

    public boolean isCompleted() { return this.completed; }

    public boolean isEmpty() {

        if (!this.completed) {
            throw new RuntimeException("a sequence group's empty status should only be queried once it is completed");
        }

        return this.sequencesWritten < 1;
    }
}
