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
    public final HashMap<String, BufferedWriter> gzippedByteArrayWriterByReadType = new HashMap<>();
    private boolean completed = false;

    public CompressedSequenceGroup(Set<String> readTypeSet) throws IOException {

        for (String readType : readTypeSet) {

            // create the byte array and buffered writer
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(8192);
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(gzipOutputStream);
            BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);

            // store the byte array output stream
            this.byteArrayByReadType.put(readType, byteArrayOutputStream);

            // store the buffered writer
            this.gzippedByteArrayWriterByReadType.put(readType, bufferedWriter);
        }
    }

    public void addSequence(String readType, SequenceLines sequenceLines) throws IOException {

        if (this.completed) { throw new RuntimeException("cannot add sequence lines to a completed sequence group"); }

        BufferedWriter writer = this.gzippedByteArrayWriterByReadType.get(readType);

        writer.write(sequenceLines.line1);
        writer.newLine();

        writer.write(sequenceLines.line2);
        writer.newLine();

        writer.write(sequenceLines.line3);
        writer.newLine();

        writer.write(sequenceLines.line4);
        writer.newLine();
    }

    public void markCompleted() throws IOException {

        if (this.completed) { throw new RuntimeException("a sequence group should not be marked completed twice"); }

        // flush all the writers
        for (BufferedWriter writer : this.gzippedByteArrayWriterByReadType.values()) { writer.flush(); }

        this.completed = true;
    }

    public boolean isCompleted() { return this.completed; }
}
