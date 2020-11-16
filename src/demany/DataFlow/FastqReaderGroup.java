package demany.DataFlow;

import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FastqReaderGroup {

    public final HashMap<String, BufferedReader> readerByReadType = new HashMap<>();
    public final int sequenceChunkSize;
    public boolean doneReading = false;

    public FastqReaderGroup(Map<String, Fastq> fastqByReadType, int sequenceChunkSize) throws IOException {

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
        if (this.doneReading) { throw new RuntimeException("cannot read sequences after we're done reading"); }

        // create the sequence group that we will be reading in
        SequenceGroup sequenceGroup = new SequenceGroup(this.readerByReadType.keySet(), this.sequenceChunkSize);

        // read in sequences from each reader
        for (String readType : this.readerByReadType.keySet()) {

            BufferedReader reader = this.readerByReadType.get(readType);

            for (int i = 0; i < this.sequenceChunkSize; i++) {

                // read in a sequence, ie 4 lines, from a fastq
                SequenceLines sequenceLines = new SequenceLines(
                        reader.readLine(), reader.readLine(), reader.readLine(), reader.readLine()
                );

                // check to see if we are done reading
                if (sequenceLines.line1 == null) {

                    // make sure that all lines are null
                    if (!sequenceLines.allLinesAreNull()) {
                        throw new RuntimeException("a partial set of 4 sequence lines was read");
                    }

                    this.doneReading = true;
                    reader.close();
                    break;
                }

                // add the sequence to the sequence group that we are reading
                sequenceGroup.addSequence(readType, sequenceLines);
            }
        }

        sequenceGroup.markCompleted();

        return sequenceGroup;
    }
}
