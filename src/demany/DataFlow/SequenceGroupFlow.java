package demany.DataFlow;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class SequenceGroupFlow {

    static class LockSequenceGroupQueuePair {

        final ReentrantLock lock = new ReentrantLock();
        final Queue<SequenceGroup> sequenceGroups = new LinkedList<>();
    }

    private final int readerGroups;
    private int readerGroupsClosed;
    private final HashMap<String, LockSequenceGroupQueuePair> inSequenceGroupsByLaneStr = new HashMap<>();
    private final HashMap<String, HashMap<String, LockSequenceGroupQueuePair>> outSeqGroupsByIdByLaneStr = new HashMap<>();
    private final int maxSequenceGroupsAllowed;

    public SequenceGroupFlow(Set<String> idSet, Set<String> laneStrSet, int maxSequenceGroupsAllowed) {

        // we will have the same number of reader threads as lanes
        this.readerGroups = laneStrSet.size();

        this.maxSequenceGroupsAllowed = maxSequenceGroupsAllowed;

        // initialize the multiplexed sequence groups
        for (String laneStr : laneStrSet) {

            this.inSequenceGroupsByLaneStr.put(laneStr, new LockSequenceGroupQueuePair());
        }

        // initialize the demultiplexed sequence groups
        for (String laneStr : laneStrSet) {
            this.outSeqGroupsByIdByLaneStr.put(laneStr, new HashMap<>());

            for (String id : idSet) {
                this.outSeqGroupsByIdByLaneStr.get(id).put(laneStr, new LockSequenceGroupQueuePair());
            }
        }
    }

    public void incrementReaderThreadsClosed() {
        readerGroupsClosed++;
    }

    public boolean allReadThreadsClosed() {
        return readerGroups == readerGroupsClosed;
    }

    public boolean moreMultiplexedSequenceGroupsAvailable() {

        for (String laneStr : inSequenceGroupsByLaneStr.keySet()) {

            // lock not needed
            if (!inSequenceGroupsByLaneStr.get(laneStr).sequenceGroups.isEmpty())  {
                return true;
            }
        }

        return false;
    }

    public boolean moreMultiplexedSequenceGroupsNeeded(String laneStr) {

        LockSequenceGroupQueuePair pair = inSequenceGroupsByLaneStr.get(laneStr);

        // lock is not necessary
        return pair.sequenceGroups.size() < maxSequenceGroupsAllowed;
    }

    public void addMultiplexedSequenceGroup(String laneStr, SequenceGroup sequenceGroup) {

        LockSequenceGroupQueuePair pair = inSequenceGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            pair.sequenceGroups.add(sequenceGroup);

        } finally {
            pair.lock.unlock();
        }
    }

    public SequenceGroup takeMultiplexedSequenceGroup(String laneStr) {

        LockSequenceGroupQueuePair pair = inSequenceGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            if (pair.sequenceGroups.isEmpty()) {
                return null;
            } else {
                return pair.sequenceGroups.remove();
            }

        } finally {
            pair.lock.unlock();
        }
    }

    public List<String> getMultiplexedSequenceGroupLaneStrPriorityList() {

        // collect all the lane str values with multiplexed sequence groups
        LinkedList<String> laneStrList = new LinkedList<>();
        for (String laneStr : inSequenceGroupsByLaneStr.keySet()) {

            LockSequenceGroupQueuePair pair = inSequenceGroupsByLaneStr.get(laneStr);

            // lock not needed
            int queueSize = pair.sequenceGroups.size();
            if (queueSize > 0) {

                laneStrList.add(laneStr);
            }
        }

        // return a sorted list of lane str in decending order of queue size
        return laneStrList.stream()
                .sorted(Comparator.comparingInt(ls -> -inSequenceGroupsByLaneStr.get(ls).sequenceGroups.size())
                ).collect(Collectors.toList());
    }

    public boolean moreDemultiplexedSequenceGroupsNeeded(String laneStr) {

        int maxSize = 0;

        for (LockSequenceGroupQueuePair pair : outSeqGroupsByIdByLaneStr.get(laneStr).values()) {

            maxSize = Math.max(pair.sequenceGroups.size(), maxSize);
        }

        return maxSize < maxSequenceGroupsAllowed;
    }

    public void addDemultiplexedSequenceGroups(String laneStr, HashMap<String, SequenceGroup> sequenceGroupById) {

        HashMap<String, LockSequenceGroupQueuePair> pairById = outSeqGroupsByIdByLaneStr.get(laneStr);

        for (String id : sequenceGroupById.keySet()) {

            LockSequenceGroupQueuePair pair = pairById.get(id);

            // block until we get the lock on the queue
            pair.lock.lock();

            try {

                pair.sequenceGroups.add(sequenceGroupById.get(id));

            } finally {
                pair.lock.unlock();
            }
        }
    }

    public HashMap<String, SequenceGroup> takeDemultiplexedSequenceGroups(String laneStr) {

        HashMap<String, LockSequenceGroupQueuePair> pairById = outSeqGroupsByIdByLaneStr.get(laneStr);
        HashMap<String, SequenceGroup> resultMap = new HashMap<>();

        for (String id : pairById.keySet()) {

            LockSequenceGroupQueuePair pair = pairById.get(id);

            // block until we get the lock on the queue
            pair.lock.lock();

            try {

                resultMap.put(id, pair.sequenceGroups.remove());

            } finally {
                pair.lock.unlock();
            }
        }

        return resultMap;
    }
}
