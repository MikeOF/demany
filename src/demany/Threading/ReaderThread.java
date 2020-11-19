package demany.Threading;

import demany.Context.DemultiplexingContext;
import demany.Fastq.FastqReaderGroup;
import demany.Fastq.SequenceGroup;
import demany.Fastq.SequenceGroupFlow;
import demany.Utils.Utils;

import java.io.IOException;
import java.util.logging.Logger;

public class ReaderThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger( ReaderThread.class.getName() );

    private final String laneStr;
    private final SequenceGroupFlow sequenceGroupFlow;
    private final FastqReaderGroup fastqReaderGroup;

    public ReaderThread(
            String laneStr, SequenceGroupFlow sequenceGroupFlow, DemultiplexingContext demultiplexingContext
    ) throws IOException {

        this.laneStr = laneStr;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.fastqReaderGroup = new FastqReaderGroup(demultiplexingContext.masterFastqByReadTypeByLaneStr.get(laneStr));
    }

    @Override
    public void run() {

        while (this.fastqReaderGroup.isNotDoneReading()) {

            // reset the "did work" variable
            boolean didWork = false;

            // check to see if we need to read a chunk of sequences
            if (this.sequenceGroupFlow.moreMultiplexedSequenceGroupsNeeded(laneStr)) {

                try {

                    // read a group of sequences
                    SequenceGroup sequenceGroup = this.fastqReaderGroup.readSequences();

                    // make sure that the sequence group is completed
                    if (!sequenceGroup.isCompleted()) {
                        throw new RuntimeException("a reader thread recieved a sequence group that was not completed");
                    }

                    // check to see if the sequence group is empty
                    if (sequenceGroup.isEmpty()) {

                        // make sure the fastq reader group is done reading
                        if (this.fastqReaderGroup.isNotDoneReading()) {
                            throw new RuntimeException(
                                    "a fastq reader group that isn't done reading generated an empty sequence group"
                            );
                        }

                    } else {

                        // add this non-empty sequence group to the flow
                        this.sequenceGroupFlow.addMultiplexedSequenceGroup(this.laneStr, sequenceGroup);
                    }

                } catch (IOException e) {
                    throw new RuntimeException("could not read sequences: " + e.getMessage());
                }

                didWork = true;
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep();
            }
        }

        // since this thread is done reading mark that info on the sequence flow
        this.sequenceGroupFlow.markReaderThreadFinished(this.laneStr);
    }
}
