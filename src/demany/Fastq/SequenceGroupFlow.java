package demany.Fastq;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SequenceGroupFlow {

    private static final int SEQUENCE_GROUP_QUEUE_MAX_SIZE = 3;

    private static class LockSequenceGroupQueuePair {

        final ReentrantLock lock = new ReentrantLock();
        final Queue<SequenceGroup> sequenceGroupQueue = new LinkedList<>();
    }

    private static class LockSequenceGroupByIdQueuePair {

        final ReentrantLock lock = new ReentrantLock();
        final Queue<HashMap<String, CompressedSequenceGroup>> compressedSequenceGroupByIdQueue = new LinkedList<>();
    }

    private final Lock readerThreadFinishedByLaneStrLock = new ReentrantLock();
    private final Map<String, Boolean> readerThreadFinishedByLaneStr = new HashMap<>();

    private final Lock demultiplexingThreadFinishedByIdLock = new ReentrantLock();
    private final Map<Integer, Boolean> demultiplexingThreadFinishedById = new HashMap<>();

    private final Map<String, LockSequenceGroupQueuePair> multiplexedSeqGroupsByLaneStr = new HashMap<>();
    private final Map<String, LockSequenceGroupByIdQueuePair> demultiplexedSeqGroupsByLaneStr = new HashMap<>();

    private final Lock countByIndexStrByIdByLaneStrLock = new ReentrantLock();
    private final Map<String, HashMap<String, HashMap<String, Long>>> countByIndexStrByIdByLaneStr = new HashMap<>();

    public SequenceGroupFlow(Set<String> laneStrSet, Set<Integer> demultiplexingThreadIdSet) {

        // init a map to store when reader threads have completed
        for (String laneStr : laneStrSet) {
            this.readerThreadFinishedByLaneStr.put(laneStr, false);
        }

        // init a map to store when demultiplexing threads have completed
        for (int id : demultiplexingThreadIdSet) {
            this.demultiplexingThreadFinishedById.put(id, false);
        }

        // init the multiplexed sequence group by lane map
        for (String laneStr : laneStrSet) {

            this.multiplexedSeqGroupsByLaneStr.put(laneStr, new LockSequenceGroupQueuePair());
        }

        // init the demultiplexed sequence group by lane map
        for (String laneStr : laneStrSet) {
            this.demultiplexedSeqGroupsByLaneStr.put(laneStr, new LockSequenceGroupByIdQueuePair());
        }
    }

    public void markReaderThreadFinished(String laneStr) {

        this.readerThreadFinishedByLaneStrLock.lock();

        try {
            this.readerThreadFinishedByLaneStr.put(laneStr, true);

        } finally {
            this.readerThreadFinishedByLaneStrLock.unlock();
        }
    }

    public boolean allReaderThreadsFinished() {
        return this.readerThreadFinishedByLaneStr.values().stream().allMatch(Boolean::booleanValue);
    }

    public void markDemultiplexingThreadFinished(int demultiplexingThreadId) {

        this.demultiplexingThreadFinishedByIdLock.lock();

        try {
            this.demultiplexingThreadFinishedById.put(demultiplexingThreadId, true);

        } finally {
            this.demultiplexingThreadFinishedByIdLock.unlock();
        }
    }

    public boolean allDemultiplexingThreadsFinished() {
        return this.demultiplexingThreadFinishedById.values().stream().allMatch(Boolean::booleanValue);
    }

    public boolean moreMultiplexedSequenceGroupsAvailable() {

        for (String laneStr : multiplexedSeqGroupsByLaneStr.keySet()) {

            // lock not needed
            if (!multiplexedSeqGroupsByLaneStr.get(laneStr).sequenceGroupQueue.isEmpty())  {
                return true;
            }
        }

        return false;
    }

    public boolean moreMultiplexedSequenceGroupsNeeded(String laneStr) {

        LockSequenceGroupQueuePair pair = multiplexedSeqGroupsByLaneStr.get(laneStr);

        // lock is not necessary
        return pair.sequenceGroupQueue.size() < SequenceGroupFlow.SEQUENCE_GROUP_QUEUE_MAX_SIZE;
    }

    public void addMultiplexedSequenceGroup(String laneStr, SequenceGroup sequenceGroup) {

        LockSequenceGroupQueuePair pair = multiplexedSeqGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {
            pair.sequenceGroupQueue.add(sequenceGroup);

        } finally {
            pair.lock.unlock();
        }
    }

    public SequenceGroup takeMultiplexedSequenceGroup(String laneStr) {

        LockSequenceGroupQueuePair pair = multiplexedSeqGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            if (pair.sequenceGroupQueue.isEmpty()) {
                return null;
            }

            return pair.sequenceGroupQueue.remove();

        } finally {
            pair.lock.unlock();
        }
    }

    public List<String> getMultiplexedSequenceGroupLaneStrPriorityList() {

        // collect all the lane str values with multiplexed sequence groups
        LinkedList<String> laneStrList = new LinkedList<>();
        for (String laneStr : multiplexedSeqGroupsByLaneStr.keySet()) {

            LockSequenceGroupQueuePair pair = multiplexedSeqGroupsByLaneStr.get(laneStr);

            // lock not needed
            int queueSize = pair.sequenceGroupQueue.size();
            if (queueSize > 0) {

                laneStrList.add(laneStr);
            }
        }

        // return a sorted list of lane str in decending order of queue size
        return laneStrList.stream()
                .sorted(Comparator.comparingInt(ls -> -multiplexedSeqGroupsByLaneStr.get(ls).sequenceGroupQueue.size())
                ).collect(Collectors.toList());
    }

    public boolean moreDemultiplexedSequenceGroupsAvailable(String laneStr) {

        // lock not needed
        return !this.demultiplexedSeqGroupsByLaneStr.get(laneStr).compressedSequenceGroupByIdQueue.isEmpty();
    }

    public boolean moreDemultiplexedSequenceGroupsNeeded(String laneStr) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);

        // lock not needed
        return pair.compressedSequenceGroupByIdQueue.size() < SequenceGroupFlow.SEQUENCE_GROUP_QUEUE_MAX_SIZE;
    }

    public void addDemultiplexedSequenceGroups(String laneStr,
                                               HashMap<String, CompressedSequenceGroup> compressedSequenceGroupById) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            pair.compressedSequenceGroupByIdQueue.add(compressedSequenceGroupById);

        } finally {
            pair.lock.unlock();
        }
    }

    public Map<String, CompressedSequenceGroup> takeDemultiplexedSequenceGroups(String laneStr) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            if (pair.compressedSequenceGroupByIdQueue.isEmpty()) { return null; }

            return pair.compressedSequenceGroupByIdQueue.remove();

        } finally {
            pair.lock.unlock();
        }
    }

    public void submitCountByIndexStrByIdByLaneStr(
            HashMap<String, HashMap<String, HashMap<String, Long>>> inCountByIndexStrByIdByLaneStr) {

        // block until we get the lock on the map
        this.countByIndexStrByIdByLaneStrLock.lock();

        try {

            for (String laneStr : inCountByIndexStrByIdByLaneStr.keySet()) {

                // add map for this lane if necessary
                if (!this.countByIndexStrByIdByLaneStr.containsKey(laneStr)) {
                    this.countByIndexStrByIdByLaneStr.put(laneStr, new HashMap<>());
                }

                // get the maps for this lane str
                HashMap<String, HashMap<String, Long>> thisCountByIndexStrById =
                        this.countByIndexStrByIdByLaneStr.get(laneStr);

                HashMap<String, HashMap<String, Long>> inCountByIndexStrById =
                        inCountByIndexStrByIdByLaneStr.get(laneStr);

                for (String id : inCountByIndexStrById.keySet()) {

                    // add map for this id if necessary
                    if (!thisCountByIndexStrById.containsKey(id)) { thisCountByIndexStrById.put(id, new HashMap<>()); }

                    // get the maps for this id
                    HashMap<String, Long> thisCountByIndexStr = thisCountByIndexStrById.get(id);

                    HashMap<String, Long> inCountByIndexStr = inCountByIndexStrById.get(id);

                    for (String indexStr : inCountByIndexStr.keySet()) {

                        // record count for this index string
                        if (!thisCountByIndexStr.containsKey(indexStr)) {
                            thisCountByIndexStr.put(indexStr, inCountByIndexStr.get(indexStr));
                        } else {
                            thisCountByIndexStr.put(
                                    indexStr,
                                    thisCountByIndexStr.get(indexStr) + inCountByIndexStr.get(indexStr)
                            );
                        }
                    }
                }
            }

        } finally {
            this.countByIndexStrByIdByLaneStrLock.unlock();
        }
    }

    public Map<String, Map<String, Map<String, Long>>> getCountByIndexStrByIdByLaneStr() {

        // block until we get the lock on the map
        this.countByIndexStrByIdByLaneStrLock.lock();

        try {

            // create an unmodifiable view of the map
            Map<String, Map<String, Map<String, Long>>> resultCountByIndexStrByIdByLaneStr = new HashMap<>();

            // each lane string
            for (String laneStr : this.countByIndexStrByIdByLaneStr.keySet()) {

                Map<String, Map<String, Long>> resultCountByIndexStrById = new HashMap<>();

                // each sample id
                for (String id : this.countByIndexStrByIdByLaneStr.get(laneStr).keySet()) {

                    resultCountByIndexStrById.put(id,
                            Collections.unmodifiableMap(this.countByIndexStrByIdByLaneStr.get(laneStr).get(id))
                    );
                }

                resultCountByIndexStrByIdByLaneStr.put(laneStr, Collections.unmodifiableMap(resultCountByIndexStrById));
            }

            // return the map
            return resultCountByIndexStrByIdByLaneStr;

        } finally {
            this.countByIndexStrByIdByLaneStrLock.unlock();
        }
    }
}
