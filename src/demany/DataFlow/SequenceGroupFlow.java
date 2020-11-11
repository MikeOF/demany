package demany.DataFlow;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

class LockSequenceGroupQueuePair {

    final ReentrantLock lock = new ReentrantLock();
    final Queue<SequenceGroup> sequenceGroups = new LinkedList<>();
}

public class SequenceGroupFlow {

    public final HashMap<String, LockSequenceGroupQueuePair> inSequenceGroupsByLaneStr = new HashMap<>();
    public final HashMap<String, HashMap<String, LockSequenceGroupQueuePair>> outSeqGroupsByIdByLaneStr = new HashMap<>();
    public final int maxSequenceGroupsAllowed;

    public SequenceGroupFlow(Set<String> idSet, Set<String> laneStrSet, int maxSequenceGroupsAllowed) {

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

    public SequenceGroup removeMultiplexedSequenceGroup(String laneStr) {

        LockSequenceGroupQueuePair pair = inSequenceGroupsByLaneStr.get(laneStr);

        // block until we get the lock on the queue
        pair.lock.lock();

        try {

            return pair.sequenceGroups.remove();

        } finally {
            pair.lock.unlock();
        }
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
}
