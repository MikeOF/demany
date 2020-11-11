package demany.DataFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class SequenceGroup {

    public final HashMap<String, ArrayList<SequenceLines>> sequenceListByReadType = new HashMap<>();

    public SequenceGroup(Set<String> readTypeSet, int size) {

        for (String readType : readTypeSet) {

            sequenceListByReadType.put(readType, new ArrayList<>(size));
        }
    }

    public void addSequence(String readType, SequenceLines sequenceLines) {
        sequenceListByReadType.get(readType).add(sequenceLines);
    }
}
