package demany.Threading;

import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.DataFlow.SequenceLines;
import demany.SampleIndex.SampleIndexLookup;
import demany.Utils.Utils;

import java.util.HashMap;
import java.util.List;

public class DemultiplexingThread extends Thread {

    final SequenceGroupFlow sequenceGroupFlow;
    final HashMap<String, SampleIndexLookup> sampleIndexLookupByLaneStr;

    public DemultiplexingThread(SequenceGroupFlow sequenceGroupFlow,
                                HashMap<String, SampleIndexLookup> sampleIndexLookupByLaneStr) {

        this.sequenceGroupFlow = sequenceGroupFlow;
        this.sampleIndexLookupByLaneStr = sampleIndexLookupByLaneStr;
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
                        HashMap<String, SequenceGroup> sequenceGroupById = demultiplexSequenceGroup(
                                laneStr, sequenceGroup
                        );

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
                Utils.tryToSleep(sleepMilliseconds);
            }
        }
    }

    private HashMap<String, SequenceGroup> demultiplexSequenceGroup(String laneStr, SequenceGroup sequenceGroup) {

        HashMap<String, SequenceGroup> sequenceGroupById = new HashMap<>();

        SampleIndexLookup lookup = sampleIndexLookupByLaneStr.get(laneStr);



        return sequenceGroupById;
    }
}
