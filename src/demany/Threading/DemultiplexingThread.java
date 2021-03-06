package demany.Threading;

import demany.Context.DemultiplexingContext;
import demany.Fastq.CompressedSequenceGroup;
import demany.Fastq.SequenceGroup;
import demany.Fastq.SequenceGroupFlow;
import demany.Fastq.SequenceLines;
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
    final DemultiplexingContext demultiplexingContext;

    public DemultiplexingThread(int demultiplexingThreadId, SequenceGroupFlow sequenceGroupFlow,
                                DemultiplexingContext demultiplexingContext) {

        this.demultiplexingThreadId = demultiplexingThreadId;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.demultiplexingContext = demultiplexingContext;

        // initialize the count by index str by id map
        for (String laneStr : demultiplexingContext.sampleIdDataSetByLaneStr.keySet()) {

            this.countByIndexStrByIdByLaneStr.put(laneStr, new HashMap<>());

            this.countByIndexStrByIdByLaneStr.get(laneStr).put(DemultiplexingContext.UNDETERMINED_ID, new HashMap<>());

            for (DemultiplexingContext.SampleIdData sampleIdData :
                    demultiplexingContext.sampleIdDataSetByLaneStr.get(laneStr)) {

                this.countByIndexStrByIdByLaneStr.get(laneStr).put(sampleIdData.id, new HashMap<>());
            }
        }
    }

    @Override
    public void run() {

        while (true) {

            // reset the "did work" variable
            boolean didWork = false;

            // get a list of lanes in order of their urgency to be processed
            List<String> laneStrList = this.sequenceGroupFlow.getMultiplexedSequenceGroupLaneStrPriorityList();

            for (String laneStr : laneStrList) {

                // demultiplex a group of sequences if needed for this lane
                if (this.sequenceGroupFlow.moreDemultiplexedSequenceGroupsNeeded(laneStr)) {

                    // get a multiplexed sequence group from this lane
                    SequenceGroup sequenceGroup = this.sequenceGroupFlow.takeMultiplexedSequenceGroup(laneStr);

                    if (sequenceGroup == null) { continue; }  // try next lane

                    try {

                        // demultiplex this sequence group
                        HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById = demultiplexSequenceGroup(
                                laneStr, sequenceGroup
                        );

                        // put the demultiplexed sequence groups in their out queues
                        this.sequenceGroupFlow.addDemultiplexedSequenceGroups(laneStr, compressedSequenceGroupById);

                    } catch (IOException e) {
                        throw new RuntimeException(
                                "could not create the compressed sequence group by id: " + e.getMessage()
                        );
                    }

                    // mark that we did work
                    didWork = true;

                    // break from the for loop
                    break;
                }
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep();

                // check to see if we are finished
                if (this.sequenceGroupFlow.allReaderThreadsFinished()
                        && !this.sequenceGroupFlow.moreMultiplexedSequenceGroupsAvailable()) {

                    break;
                }
            }
        }

        // submit this thread's index counts to the sequence group flow
        this.sequenceGroupFlow.submitCountByIndexStrByIdByLaneStr(this.countByIndexStrByIdByLaneStr);

        // mark that this thread has finished on the sequence group flow
        this.sequenceGroupFlow.markDemultiplexingThreadFinished(this.demultiplexingThreadId);
    }

    private HashMap<String, CompressedSequenceGroup> demultiplexSequenceGroup(
            String laneStr, SequenceGroup sequenceGroup) throws IOException {

        // initialize the sequence group by id map
        HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById = new HashMap<>();

        compressedSequenceGroupById.put(
                DemultiplexingContext.UNDETERMINED_ID,
                new CompressedSequenceGroup(this.demultiplexingContext.readTypeSet)
        );

        for (DemultiplexingContext.SampleIdData sampleIdData :
                this.demultiplexingContext.sampleIdDataSetByLaneStr.get(laneStr)) {

            compressedSequenceGroupById.put(
                    sampleIdData.id,
                    new CompressedSequenceGroup(this.demultiplexingContext.readTypeSet)
            );
        }

        // get the count by index-str by id for this lane
        HashMap<String, HashMap<String, Long>> countByIndexStrById = this.countByIndexStrByIdByLaneStr.get(laneStr);

        // get the lookup for this lane
        SampleIndexLookup lookup = this.demultiplexingContext.sampleIndexLookupByLaneStr.get(laneStr);

        // demultiplex the input sequence group
        SequenceLines index2SeqLines;
        String index2 = null;
        for (int i = 0; i < sequenceGroup.size(); i++) {

            // get this index lines and index strings
            SequenceLines index1SeqLines =
                    sequenceGroup.sequenceListByReadType.get(this.demultiplexingContext.index1ReadType).get(i);
            String index1 = index1SeqLines.line2.substring(0, this.demultiplexingContext.index1Length);

            if (this.demultiplexingContext.hasIndex2) {

                index2SeqLines =
                        sequenceGroup.sequenceListByReadType.get(this.demultiplexingContext.index2ReadType).get(i);
                index2 = index2SeqLines.line2.substring(0, this.demultiplexingContext.index2Length);
            }

            // lookup the sample id
            String sampleId = lookup.lookupProjectSampleId(index1, index2);

            // set the undetermined id if we didn't find a sample id
            if (sampleId == null) { sampleId = DemultiplexingContext.UNDETERMINED_ID; }

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
            if (this.demultiplexingContext.hasIndex2) { indexStr = index1 + "-" + index2; }
            else { indexStr = index1; }

            // record the index count
            if (!countByIndexStr.containsKey(indexStr)) {
                countByIndexStr.put(indexStr, 1L);
            } else {
                countByIndexStr.put(indexStr, countByIndexStr.get(indexStr) + 1L);
            }
        }

        // mark each sequence group complete
        for (String id : compressedSequenceGroupById.keySet()) {

            compressedSequenceGroupById.get(id).markCompleted();
        }

        // remove empty compressed sequence groups
        for (String id : compressedSequenceGroupById.keySet().toArray(new String[0])) {

            if (compressedSequenceGroupById.get(id).isEmpty()) { compressedSequenceGroupById.remove(id); }
        }

        return compressedSequenceGroupById;
    }
}
