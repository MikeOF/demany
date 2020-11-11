package demany.Threading;

import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.SampleIndex.SampleIndexLookup;

import java.util.HashMap;
import java.util.List;

public class DemultiplexingThread extends Thread {

    final SequenceGroupFlow sequenceGroupFlow;

    public DemultiplexingThread(SequenceGroupFlow sequenceGroupFlow, HashMap<String, SampleIndexLookup> sampleIndexLookupByLaneStr) {
        this.sequenceGroupFlow = sequenceGroupFlow;
    }

    public void run() {

        long sleepMilliseconds = 100;

        while (true) {
            boolean didWork = false;

            // get a list of lanes in order of their urgency to be processed
            List<String> laneStrList = sequenceGroupFlow.getMultiplexedSequenceGroupLaneStrPriorityList();

            if (!laneStrList.isEmpty()) {

                for (String laneStr : laneStrList) {

                    // demultiplex a group of sequences if needed for this lane
                    if (sequenceGroupFlow.moreDemultiplexedSequenceGroupsNeeded(laneStr)) {

                        // get a multiplexed sequence group from this lane
                        SequenceGroup sequenceGroup = sequenceGroupFlow.takeMultiplexedSequenceGroup(laneStr);

                        if (sequenceGroup == null) { continue; }  // try nex lane

                        // demultiplex this sequence group
                        HashMap<String, SequenceGroup> sequenceGroupById = demultiplexSequenceGroup(sequenceGroup);

                        // put the demultiplexed sequence groups in their out queues
                        sequenceGroupFlow.addDemultiplexedSequenceGroups(laneStr, sequenceGroupById);

                        // mark that we did work
                        didWork = true;
                    }
                }
            }

            if (!didWork) {

                // before we sleep check to see if we are actually finished
                if (sequenceGroupFlow.allReadThreadsClosed() && !sequenceGroupFlow.moreMultiplexedSequenceGroupsAvailable()) {
                    break;
                }

                // sleep a bit
                tryToSleep(sleepMilliseconds);
            }
        }
    }

    private void tryToSleep(long sleepMilliseconds) {
        try {
            sleep(sleepMilliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException("could not sleep: " + e.getMessage());
        }
    }

    private static HashMap<String, SequenceGroup> demultiplexSequenceGroup(SequenceGroup sequenceGroup) {

        HashMap<String, SequenceGroup> sequenceGroupById = new HashMap<>();

        return sequenceGroupById;
    }
}
