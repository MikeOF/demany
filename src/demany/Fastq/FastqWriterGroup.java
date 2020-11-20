package demany.Fastq;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FastqWriterGroup {

    public final Map<String, FileOutputStream> fileOutputStreamByReadType;

    public FastqWriterGroup(Map<String, Fastq> fastqByReadType) throws IOException {

        // get a writer for each fastq path passed in
        Map<String, FileOutputStream> tempFileOutputStreamByReadType = new HashMap<>();
        for (String readType : fastqByReadType.keySet()) {

            tempFileOutputStreamByReadType.put(
                    readType, new FileOutputStream(fastqByReadType.get(readType).path.toString())
            );
        }

        this.fileOutputStreamByReadType = Collections.unmodifiableMap(tempFileOutputStreamByReadType);
    }

    public void writeSequences(CompressedSequenceGroup compressedSequenceGroup) throws IOException {

        // make sure the compressed sequence group has completed
        if (!compressedSequenceGroup.isCompleted()) {
            throw new RuntimeException("attempted to write an incomplete compressed sequence group to file");
        }

        // write out the sequences
        for (String readType : compressedSequenceGroup.byteArrayByReadType.keySet()) {

            // get the byte array output stream
            ByteArrayOutputStream byteArrayOutputStream = compressedSequenceGroup.byteArrayByReadType.get(readType);

            // get the file output stream
            FileOutputStream fileOutputStream = this.fileOutputStreamByReadType.get(readType);

            // write the byte array output stream's bytes to the file output stream
            byteArrayOutputStream.writeTo(fileOutputStream);
        }
    }

    public void close() throws IOException {

        // close all the writers
        for (FileOutputStream fileOutputStream : this.fileOutputStreamByReadType.values()) { fileOutputStream.close(); }
    }
}
