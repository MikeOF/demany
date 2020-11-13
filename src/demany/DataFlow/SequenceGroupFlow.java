package demany.DataFlow;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SequenceGroupFlow {

    static class LockSequenceGroupQueuePair {

        final ReentrantLock lock = new ReentrantLock();
        final Queue<SequenceGroup> sequenceGroupQueue = new LinkedList<>();
    }

    static class LockSequenceGroupByIdQueuePair {

        final ReentrantLock lock = new ReentrantLock();
        final Queue<HashMap<String, SequenceGroup>> sequenceGroupByIdQueue = new LinkedList<>();
    }

    private final Lock readerThreadFinishedByLaneStrLock = new ReentrantLock();
    private final HashMap<String, Boolean> readerThreadFinishedByLaneStr = new HashMap<>();

    private final Lock demultiplexingThreadFinishedByIdLock = new ReentrantLock();
    private final HashMap<Integer, Boolean> demultiplexingThreadFinishedById = new HashMap<>();

    private final Lock writerThreadFinishedByLaneStrLock = new ReentrantLock();
    private final HashMap<String, Boolean> writerThreadFinishedByLaneStr = new HashMap<>();

    private final HashMap<String, LockSequenceGroupQueuePair> multiplexedSeqGroupsByLaneStr = new HashMap<>();
    private final HashMap<String, LockSequenceGroupByIdQueuePair> demultiplexedSeqGroupsByLaneStr = new HashMap<>();
    private final int maxSequenceGroupsAllowed;

    public SequenceGroupFlow(Set<String> laneStrSet, int maxSequenceGroupsAllowed,
                             Set<Integer> demultiplexingThreadIdSet) {

        this.maxSequenceGroupsAllowed = maxSequenceGroupsAllowed;

        // create a map to store when reader threads have completed
        for (String laneStr : laneStrSet) {
            this.readerThreadFinishedByLaneStr.put(laneStr, false);
        }

        // create a map to store when demultiplexing threads have completed
        for (int id : demultiplexingThreadIdSet) {
            this.demultiplexingThreadFinishedById.put(id, false);
        }

        // create a map to store when writer threads have completed
        for (String laneStr : laneStrSet) {
            this.writerThreadFinishedByLaneStr.put(laneStr, false);
        }

        // initialize the multiplexed sequence groups
        for (String laneStr : laneStrSet) {

            this.multiplexedSeqGroupsByLaneStr.put(laneStr, new LockSequenceGroupQueuePair());
        }

        // initialize the demultiplexed sequence groups
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

    public void markWriterThreadFinished(String laneStr) {

        this.writerThreadFinishedByLaneStrLock.lock();

        try {
            this.writerThreadFinishedByLaneStr.put(laneStr, true);

        } finally {
            this.writerThreadFinishedByLaneStrLock.unlock();
        }
    }

    public boolean allWriterThreadsFinished() {
        return this.writerThreadFinishedByLaneStr.values().stream().allMatch(Boolean::booleanValue);
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
        return pair.sequenceGroupQueue.size() < maxSequenceGroupsAllowed;
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

    public boolean moreDemultiplexedSequenceGroupsAvailable() {

        for (String laneStr : this.demultiplexedSeqGroupsByLaneStr.keySet()) {

            // lock not needed
            if (!this.demultiplexedSeqGroupsByLaneStr.get(laneStr).sequenceGroupByIdQueue.isEmpty())  {
                return true;
            }
        }

        return false;
    }

    public boolean moreDemultiplexedSequenceGroupsNeeded(String laneStr) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);

        // lock not needed
        return pair.sequenceGroupByIdQueue.size() < this.maxSequenceGroupsAllowed;
    }

    public void addDemultiplexedSequenceGroups(String laneStr, HashMap<String, SequenceGroup> sequenceGroupById) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            pair.sequenceGroupByIdQueue.add(sequenceGroupById);

        } finally {
            pair.lock.unlock();
        }
    }

    public HashMap<String, SequenceGroup> takeDemultiplexedSequenceGroups(String laneStr) {

        LockSequenceGroupByIdQueuePair pair = this.demultiplexedSeqGroupsByLaneStr.get(laneStr);


        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            if (pair.sequenceGroupByIdQueue.isEmpty()) {
                return null;
            }

            return pair.sequenceGroupByIdQueue.remove();

        } finally {
            pair.lock.unlock();
        }
    }
}
