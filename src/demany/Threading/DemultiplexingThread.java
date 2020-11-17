package demany.Threading;

import demany.Context;
import demany.DataFlow.CompressedSequenceGroup;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.DataFlow.SequenceLines;
import demany.SampleIndex.SampleIndexLookup;
import demany.Utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

public class DemultiplexingThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger( DemultiplexingThread.class.getName() );

    final HashMap<String, HashMap<String, HashMap<String, Long>>> countByIndexStrByIdByLaneStr = new HashMap<>();
    final int demultiplexingThreadId;
    final SequenceGroupFlow sequenceGroupFlow;
    final Context context;

    public DemultiplexingThread(int demultiplexingThreadId, SequenceGroupFlow sequenceGroupFlow, Context context) {

        this.demultiplexingThreadId = demultiplexingThreadId;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.context = context;

        // initialize the count by index str by id map
        for (String laneStr : context.sampleIdDataSetByLaneStr.keySet()) {

            this.countByIndexStrByIdByLaneStr.put(laneStr, new HashMap<>());

            for (Context.SampleIdData sampleIdData : context.sampleIdDataSetByLaneStr.get(laneStr)) {

                this.countByIndexStrByIdByLaneStr.get(laneStr).put(sampleIdData.id, new HashMap<>());
            }
        }
    }

    @Override
    public void run() {

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

                        try {

                            // demultiplex this sequence group
                            HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById = demultiplexSequenceGroup(
                                    laneStr, sequenceGroup
                            );

                            // put the demultiplexed sequence groups in their out queues
                            sequenceGroupFlow.addDemultiplexedSequenceGroups(laneStr, compressedSequenceGroupById);

                        } catch (IOException e) {
                            throw new RuntimeException("could not create the compressed sequence group by id: " + e.getMessage());
                        }

                        // mark that we did work
                        didWork = true;
                    }
                }
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep();

                // check to see if we are finished
                if (sequenceGroupFlow.allReaderThreadsFinished() &&
                        !sequenceGroupFlow.moreMultiplexedSequenceGroupsAvailable()) {

                    break;
                }

            }
        }

        // submit this threads index counts to the sequence group flow
        this.sequenceGroupFlow.submitCountByIndexStrByIdByLaneStr(this.countByIndexStrByIdByLaneStr);

        // mark that this thread has finished on the sequence group flow
        this.sequenceGroupFlow.markDemultiplexingThreadFinished(this.demultiplexingThreadId);
    }

    private HashMap<String, CompressedSequenceGroup> demultiplexSequenceGroup(String laneStr, SequenceGroup sequenceGroup) throws IOException {

        // initialize the sequence group by id map
        HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById = new HashMap<>();

        compressedSequenceGroupById.put(
                Context.undeterminedId, new CompressedSequenceGroup(this.context.readTypeSet)
        );

        for (Context.SampleIdData sampleIdData : this.context.sampleIdDataSetByLaneStr.get(laneStr)) {
            compressedSequenceGroupById.put(
                    sampleIdData.id, new CompressedSequenceGroup(this.context.readTypeSet)
            );
        }

        // get the count by index-str by id for this lane
        HashMap<String, HashMap<String, Long>> countByIndexStrById = this.countByIndexStrByIdByLaneStr.get(laneStr);

        // get the lookup for this lane
        SampleIndexLookup lookup = context.sampleIndexLookupByLaneStr.get(laneStr);

        // demultiplex the input sequence group
        SequenceLines index2SeqLines;
        String index2 = null;
        for (int i = 0; i < sequenceGroup.size(); i++) {

            // get this index lines and index strings
            SequenceLines index1SeqLines = sequenceGroup.sequenceListByReadType.get(this.context.index1ReadType).get(i);
            String index1 = index1SeqLines.line2.substring(0, this.context.index1Length);

            if (this.context.hasIndex2) {

                index2SeqLines = sequenceGroup.sequenceListByReadType.get(this.context.index2ReadType).get(i);
                index2 = index2SeqLines.line2.substring(0, this.context.index2Length);
            }

            // lookup the sample id
            String sampleId = lookup.lookupProjectSampleId(index1, index2);

            // set the undetermined id if we didn't find a sample id
            if (sampleId == null) { sampleId = Context.undeterminedId; }

            // add lines to sequence group
            for (String readTypeString : sequenceGroup.sequenceListByReadType.keySet()) {
                compressedSequenceGroupById.get(sampleId).addSequence(
                        readTypeString,
                        sequenceGroup.sequenceListByReadType.get(readTypeString).get(i)
                );
            }

            // get the count by index str map for this sample id
            HashMap<String, Long> countByIndexStr = countByIndexStrById.get(sampleId);

            // get the index string
            String indexStr;
            if (this.context.hasIndex2) { indexStr = index1 + "-" + index2; } else { indexStr = index1; }

            // record the index count
            if (!countByIndexStr.containsKey(indexStr)) {
                countByIndexStr.put(indexStr, 1L);
            } else {
                countByIndexStr.put(indexStr, countByIndexStr.get(indexStr) + 1L);
            }
        }

        // prune and complete sequence group map
        for (String id : compressedSequenceGroupById.keySet()) {

            CompressedSequenceGroup seqGroup = compressedSequenceGroupById.get(id);

            seqGroup.markCompleted();

            if (seqGroup.isEmpty()) { compressedSequenceGroupById.remove(id); }
        }

        return compressedSequenceGroupById;
    }
}
