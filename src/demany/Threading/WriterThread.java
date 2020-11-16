package demany.Threading;

import demany.Context;
import demany.DataFlow.FastqWriterGroup;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriterThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger( WriterThread.class.getName() );

    private final String laneStr;
    private final SequenceGroupFlow sequenceGroupFlow;
    private final HashMap<String, FastqWriterGroup> fastqWriterGroupById = new HashMap<>();

    public WriterThread(String laneStr, SequenceGroupFlow sequenceGroupFlow, Context context) throws IOException {

        this.laneStr = laneStr;
        this.sequenceGroupFlow = sequenceGroupFlow;

        // add an undetermined fastq writer group for this lane
        Map<String, Map<String, Fastq>> outputFastqByReadTypeById = context.outputFastqByReadTypeByIdByLaneStr.get(laneStr);
        for (String id : outputFastqByReadTypeById.keySet()) {
            this.fastqWriterGroupById.put(
                    id,
                    new FastqWriterGroup(outputFastqByReadTypeById.get(id))
            );
        }
    }

    @Override
    public void run() {

        while(true) {
            boolean didWork = false;

            // attempt to take a collection of sequence groups
            HashMap<String, SequenceGroup> sequenceGroupById =
                    this.sequenceGroupFlow.takeDemultiplexedSequenceGroups(this.laneStr);

            // write sequences if we got em
            if (sequenceGroupById != null) {

                for (String id : sequenceGroupById.keySet()) {

                    try {
                        this.fastqWriterGroupById.get(id).writeSequences(sequenceGroupById.get(id));
                    } catch (IOException e) {
                        throw new RuntimeException("could not write sequences: " + e.getMessage());
                    }
                }

                didWork = true;
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep();

                // check to see if we are finished
                if (this.sequenceGroupFlow.allReaderThreadsFinished() &&
                        this.sequenceGroupFlow.allDemultiplexingThreadsFinished() &&
                        !this.sequenceGroupFlow.moreDemultiplexedSequenceGroupsAvailable()) {

                    break;
                }

            }
        }

        // mark that this thread has finished
        this.sequenceGroupFlow.markWriterThreadFinished(this.laneStr);
    }
}
