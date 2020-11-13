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

public class WriterThread extends Thread {

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

        long sleepMilliseconds = 100;

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
                Utils.tryToSleep(sleepMilliseconds);

                // check to see if we are finished
                if (this.sequenceGroupFlow.allReadThreadsFinished() &&
                        this.sequenceGroupFlow.allDemultiplexingThreadsFinished() &&
                        !this.sequenceGroupFlow.moreDemultiplexedSequenceGroupsAvailable()) {

                    break;
                }

                // increase sleep milliseconds
                sleepMilliseconds = (sleepMilliseconds + 10);

            } else {

                // decrease sleep milliseconds
                if (sleepMilliseconds > 20) { sleepMilliseconds = sleepMilliseconds - 10; }
            }
        }
    }
}
