package demany.Threading;

import demany.Context.DemultiplexingContext;
import demany.Fastq.CompressedSequenceGroup;
import demany.Fastq.FastqWriterGroup;
import demany.Fastq.SequenceGroupFlow;
import demany.Fastq.Fastq;
import demany.Utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class WriterThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger( WriterThread.class.getName() );

    private final String laneStr;
    private final SequenceGroupFlow sequenceGroupFlow;
    private final HashMap<String, FastqWriterGroup> fastqWriterGroupById = new HashMap<>();

    public WriterThread(String laneStr, SequenceGroupFlow sequenceGroupFlow, DemultiplexingContext demultiplexingContext) throws IOException {

        this.laneStr = laneStr;
        this.sequenceGroupFlow = sequenceGroupFlow;

        // add an undetermined fastq writer group for this lane
        Map<String, Map<String, Fastq>> outputFastqByReadTypeById = demultiplexingContext.outputFastqByReadTypeByIdByLaneStr.get(laneStr);
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
            HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById =
                    this.sequenceGroupFlow.takeDemultiplexedSequenceGroups(this.laneStr);

            // write sequences if we got em
            if (compressedSequenceGroupById != null) {

                for (String id : compressedSequenceGroupById.keySet()) {

                    try {
                        this.fastqWriterGroupById.get(id).writeSequences(compressedSequenceGroupById.get(id));
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
                if (this.sequenceGroupFlow.allDemultiplexingThreadsFinished() &&
                        !this.sequenceGroupFlow.moreDemultiplexedSequenceGroupsAvailable()) {

                    break;
                }

            }
        }

        // mark that this thread has finished
        this.sequenceGroupFlow.markWriterThreadFinished(this.laneStr);
    }
}
