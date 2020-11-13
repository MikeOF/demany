package demany.DataFlow;

import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FastqWriterGroup {

    public final HashMap<String, BufferedWriter> writerByReadType = new HashMap<>();

    public FastqWriterGroup(Map<String, Fastq> fastqByReadType) throws IOException {

        // get a writer for each fastq path passed in
        for (String readType : fastqByReadType.keySet()) {

            this.writerByReadType.put(
                    readType,
                    Utils.getBufferedGzippedFileWriter(fastqByReadType.get(readType).path)
            );
        }
    }

    public void writeSequences(SequenceGroup sequenceGroup) throws IOException {

        // write out the sequences
        for (String readType : sequenceGroup.sequenceListByReadType.keySet()) {

            BufferedWriter writer = writerByReadType.get(readType);

            for (SequenceLines sequenceLines : sequenceGroup.sequenceListByReadType.get(readType)) {

                writer.write(sequenceLines.line1);
                writer.write(sequenceLines.line2);
                writer.write(sequenceLines.line3);
                writer.write(sequenceLines.line4);
            }
        }
    }

    public void close() throws IOException {

        // close all the writers
        for (BufferedWriter writer : writerByReadType.values()) {
            writer.close();
        }
    }
}
