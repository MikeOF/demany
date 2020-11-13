package demany.Threading;

import demany.Context;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.DataFlow.SequenceLines;
import demany.SampleIndex.SampleIndexLookup;
import demany.Utils.Utils;

import java.util.HashMap;
import java.util.List;

public class DemultiplexingThread extends Thread {

    final int demultiplexingThreadId;
    final SequenceGroupFlow sequenceGroupFlow;
    final Context context;

    public DemultiplexingThread(int demultiplexingThreadId, SequenceGroupFlow sequenceGroupFlow, Context context) {

        this.demultiplexingThreadId = demultiplexingThreadId;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.context = context;
    }

    @Override
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

                // sleep a bit
                Utils.tryToSleep(sleepMilliseconds);

                // check to see if we are finished
                if (sequenceGroupFlow.allReadThreadsFinished() &&
                        !sequenceGroupFlow.moreMultiplexedSequenceGroupsAvailable()) {

                    break;
                }

                // increase sleep milliseconds
                sleepMilliseconds = (sleepMilliseconds + 10);

            } else {

                // decrease sleep milliseconds
                if (sleepMilliseconds > 20) { sleepMilliseconds = sleepMilliseconds - 10; }
            }
        }

        // mark that this thread has finished on the sequence group flow
        this.sequenceGroupFlow.markDemultiplexingThreadFinished(this.demultiplexingThreadId);
    }

    private HashMap<String, SequenceGroup> demultiplexSequenceGroup(String laneStr, SequenceGroup sequenceGroup) {

        // initialize the sequence group by id map
        HashMap<String, SequenceGroup> sequenceGroupById = new HashMap<>();

        sequenceGroupById.put(
                Context.undeterminedId,
                new SequenceGroup(this.context.readTypeSet, this.context.demultiplexedSequenceGroupSize)
        );

        for (Context.SampleIdData sampleIdData : this.context.sampleIdDataSetByLaneStr.get(laneStr)) {
            sequenceGroupById.put(
                    sampleIdData.id,
                    new SequenceGroup(this.context.readTypeSet, this.context.demultiplexedSequenceGroupSize)
            );
        }

        // get the lookup for this lane
        SampleIndexLookup lookup = context.sampleIndexLookupByLaneStr.get(laneStr);

        // demultiplex the input sequence group
        SequenceLines index2SeqLines = null;
        String index2 = null;
        for (int i = 0; i < sequenceGroup.size(); i++) {

            // get this index lines and index strings
            SequenceLines index1SeqLines = sequenceGroup.sequenceListByReadType.get(this.context.index1ReadTypeStr).get(i);
            String index1 = index1SeqLines.line2.substring(0, this.context.index1Length);

            if (this.context.hasIndex2) {

                index2SeqLines = sequenceGroup.sequenceListByReadType.get(this.context.index2ReadTypeStr).get(i);
                index2 = index2SeqLines.line2.substring(0, this.context.index2Length);
            }

            // lookup the sample id
            String sampleId = lookup.lookupProjectSampleId(index1, index2);

            if (sampleId == null) { sampleId = Context.undeterminedId; }

            // add lines to sequence group
            for (String readTypeString : sequenceGroup.sequenceListByReadType.keySet()) {
                sequenceGroupById.get(sampleId).addSequence(
                        readTypeString,
                        sequenceGroup.sequenceListByReadType.get(readTypeString).get(i)
                );
            }
        }

        // prune and complete sequence group map
        for (String id : sequenceGroupById.keySet()) {

            SequenceGroup seqGroup = sequenceGroupById.get(id);

            seqGroup.markCompleted();

            if (seqGroup.isEmpty()) { sequenceGroupById.remove(id); }
        }

        return sequenceGroupById;
    }
}
