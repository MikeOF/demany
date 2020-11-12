package demany.DataFlow;

import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;

public class FastqReaderGroup {

    public final HashMap<String, BufferedReader> readerByReadType = new HashMap<>();
    public final int sequenceChunkSize;
    public boolean doneReading = false;

    public FastqReaderGroup(HashMap<String, Fastq> fastqByReadType, int sequenceChunkSize) throws IOException {

        // get a reader for each fastq passed in
        for (String readType : fastqByReadType.keySet()) {

            this.readerByReadType.put(
                    readType,
                    Utils.getBufferedGzippedFileReader(fastqByReadType.get(readType).path)
            );
        }

        this.sequenceChunkSize = sequenceChunkSize;
    }

    public SequenceGroup readSequences() throws IOException {

        // make sure we haven't already finished reading
        if (doneReading) {
            throw new RuntimeException("cannot read sequences after we're done reading");
        }

        // create the sequence group that we will be reading in
        SequenceGroup sequenceGroup = new SequenceGroup(readerByReadType.keySet(), sequenceChunkSize);

        // read in sequences from each reader
        for (String readType : readerByReadType.keySet()) {

            BufferedReader reader = readerByReadType.get(readType);

            for (int i = 0; i < sequenceChunkSize; i++) {

                // read in a sequence, ie 4 lines, from a fastq
                SequenceLines sequenceLines = new SequenceLines(
                        reader.readLine(),
                        reader.readLine(),
                        reader.readLine(),
                        reader.readLine()
                );

                // check to see if we are done reading
                if (sequenceLines.isNull()) {
                    this.doneReading = true;
                    reader.close();
                    continue;
                }

                // add the sequence to the sequence group that we are reading
                sequenceGroup.addSequence(readType, sequenceLines);
            }
        }

        sequenceGroup.markCompleted();

        return sequenceGroup;
    }
}
