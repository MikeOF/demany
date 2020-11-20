package demany.Fastq;

import demany.Utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FastqReaderGroup {

    private final Map<String, BufferedReader> readerByReadType;
    private boolean doneReading = false;

    public FastqReaderGroup(Map<String, Fastq> fastqByReadType) throws IOException {

        // get a reader for each fastq passed in
        Map<String, BufferedReader> tempReaderByReadType = new HashMap<>();
        for (String readType : fastqByReadType.keySet()) {

            tempReaderByReadType.put(
                    readType, Utils.getBufferedGzippedFileReader(fastqByReadType.get(readType).path)
            );
        }

        this.readerByReadType = Collections.unmodifiableMap(tempReaderByReadType);
    }

    public SequenceGroup readSequences() throws IOException {

        // make sure we haven't already finished reading
        if (this.doneReading) { throw new RuntimeException("cannot read sequences after we're done reading"); }

        // create the sequence group that we will be reading in
        SequenceGroup sequenceGroup = new SequenceGroup(this.readerByReadType.keySet());

        // read in sequences from each reader
        for (String readType : this.readerByReadType.keySet()) {

            BufferedReader reader = this.readerByReadType.get(readType);

            for (int i = 0; i < SequenceGroup.MAX_NUMBER_OF_SEQUENCES; i++) {

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

    public boolean isNotDoneReading() { return !this.doneReading; }
}
