package demany.Threading;

import demany.DataFlow.FastqReaderGroup;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.Utils.Utils;

import java.io.IOException;

public class ReaderThread extends Thread {

    private final String laneStr;
    private final FastqReaderGroup fastqReaderGroup;
    private final SequenceGroupFlow sequenceGroupFlow;

    public ReaderThread(SequenceGroupFlow sequenceGroupFlow, String laneStr, FastqReaderGroup fastqReaderGroup) {

        this.sequenceGroupFlow = sequenceGroupFlow;
        this.laneStr = laneStr;
        this.fastqReaderGroup = fastqReaderGroup;
    }

    public void run() {

        long sleepMilliseconds = 100;

        while (!fastqReaderGroup.doneReading) {
            boolean didWork = false;

            // check to see if we need to read a chunk of sequences
            if (sequenceGroupFlow.moreMultiplexedSequenceGroupsNeeded(laneStr)) {

                try {

                    // read a group of sequences
                    SequenceGroup sequenceGroup = fastqReaderGroup.readSequences();

                    sequenceGroupFlow.addMultiplexedSequenceGroup(laneStr, sequenceGroup);

                } catch (IOException e) {
                    throw new RuntimeException("could not read sequences: " + e.getMessage());
                }

                didWork = true;
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep(sleepMilliseconds);
            }
        }
    }
}
