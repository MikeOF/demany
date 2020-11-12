package demany.DataFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class SequenceGroup {

    public final HashMap<String, ArrayList<SequenceLines>> sequenceListByReadType = new HashMap<>();
    private boolean completed = false;
    private int size = 0;

    public SequenceGroup(Set<String> readTypeSet, int size) {

        for (String readType : readTypeSet) {

            sequenceListByReadType.put(readType, new ArrayList<>(size));
        }
    }

    public void addSequence(String readType, SequenceLines sequenceLines) {

        if (this.completed) {
            throw new RuntimeException("cannot add sequence lines to a completed sequence group");
        }

        sequenceListByReadType.get(readType).add(sequenceLines);
    }

    public void markCompleted() {

        if (this.completed) {
            throw new RuntimeException("a sequence group should not be marked completed twice");
        }

        int size = -1;
        for (ArrayList<SequenceLines> sequenceLinesArrayList : this.sequenceListByReadType.values()) {

            if (size == -1) {
                size = sequenceLinesArrayList.size();

            } else {
                if (size != sequenceLinesArrayList.size()) {
                    throw new RuntimeException(
                            "a sequence group with different sized lists of sequence lines cannot be complete"
                    );
                }
            }
        }

        this.size = size;
        this.completed = true;
    }

    public boolean isCompleted() { return this.completed; }

    public boolean isEmpty() {

        if (!this.completed) {
            throw new RuntimeException("a sequence group's empty status should only be queried once it is completed");
        }

        return this.size == 0;
    }

    public int size() {

        if (!this.completed) {
            throw new RuntimeException("a sequence group's size should only be queried once it is completed");
        }

        return this.size;
    }
}
