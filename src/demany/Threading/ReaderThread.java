package demany.Threading;

import demany.Context;
import demany.DataFlow.FastqReaderGroup;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.Utils.Utils;

import java.io.IOException;

public class ReaderThread extends Thread {

    private final String laneStr;
    private final SequenceGroupFlow sequenceGroupFlow;
    private final FastqReaderGroup fastqReaderGroup;

    public ReaderThread(String laneStr, SequenceGroupFlow sequenceGroupFlow, Context context) throws IOException {

        this.laneStr = laneStr;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.fastqReaderGroup = new FastqReaderGroup(
                context.masterFastqByReadTypeByLaneStr.get(laneStr),
                context.mutiplexedSequenceGroupSize
        );
    }

    @Override
    public void run() {

        long sleepMilliseconds = 100;

        while (!fastqReaderGroup.doneReading) {
            boolean didWork = false;

            // check to see if we need to read a chunk of sequences
            if (sequenceGroupFlow.moreMultiplexedSequenceGroupsNeeded(laneStr)) {

                try {

                    // read a group of sequences
                    SequenceGroup sequenceGroup = fastqReaderGroup.readSequences();

                    // make sure that the sequence group is completed
                    if (!sequenceGroup.isCompleted()) {
                        throw new RuntimeException("a reader thread recieved a sequence group that was not completed");
                    }

                    // check to see if the sequence group is empty
                    if (sequenceGroup.isEmpty()) {

                        // make sure the fastq reader group is done reading
                        if (!fastqReaderGroup.doneReading) {
                            throw new RuntimeException(
                                    "a fastq reader group that isn't done reading returned an empty sequence group"
                            );
                        }

                    } else {

                        // add this non-empty sequence group to the flow
                        sequenceGroupFlow.addMultiplexedSequenceGroup(laneStr, sequenceGroup);
                    }

                } catch (IOException e) {
                    throw new RuntimeException("could not read sequences: " + e.getMessage());
                }

                didWork = true;
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep(sleepMilliseconds);

                // increase sleep milliseconds
                sleepMilliseconds = (sleepMilliseconds + 10);

            } else {

                // decrease sleep milliseconds
                if (sleepMilliseconds > 20) { sleepMilliseconds = sleepMilliseconds - 10; }
            }
        }

        // since this thread is done reading mark that info on the sequence flow
        this.sequenceGroupFlow.markReaderThreadFinished(this.laneStr);
    }
}
