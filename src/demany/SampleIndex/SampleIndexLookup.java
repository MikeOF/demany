package demany.SampleIndex;

import demany.SequenceOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class SampleIndexLookup {

    private final HashMap<String, SampleIndexSpecIndex2KeySetPair> lookupMap;

    public SampleIndexLookup(SampleIndexKeyMappingCollection sampleIndexKeyMappingCollection,
                             boolean index2ReverseCompliment) {

        // check argument
        if (sampleIndexKeyMappingCollection.hasIdentityKeyCollision()) {
            throw new RuntimeException(
                    "cannot create a lookup table with a sample index key mapping collection that has an " +
                            "identity collision"
            );
        }

        // create the lookup map
        lookupMap = new HashMap<>();
        SequenceOperator sequenceOperator = new SequenceOperator(true);

        ArrayList<SampleIndexKeyMapping> sampleIndexKeyMappingArrayList =
                sampleIndexKeyMappingCollection.getSampleIndexKeyMappingList();
        for (int i = 0; i < sampleIndexKeyMappingArrayList.size(); i++) {

            // get this mappings key maps
            SampleIndexKeyMapping keyMapping = sampleIndexKeyMappingArrayList.get(i);
            HashSet<String> index1KeySet = keyMapping.getIndex1KeySetCopy();

            HashSet<String> index2KeySet = null;
            if (keyMapping.hasIndex2()) { index2KeySet = keyMapping.getIndex2KeySetCopy(); }

            // prune the key maps to non-overlapping keys
            for (int j = 0; j < sampleIndexKeyMappingArrayList.size(); j++) {

                // skip if i and j are the same
                if (i == j) { continue; }

                // get the other key maps
                SampleIndexKeyMapping otherKeyMapping = sampleIndexKeyMappingArrayList.get(j);
                HashSet<String> otherIndex1KeySet = otherKeyMapping.getIndex1KeySetCopy();

                HashSet<String> otherIndex2KeySet = null;
                if (otherKeyMapping.hasIndex2()) { otherIndex2KeySet = otherKeyMapping.getIndex2KeySetCopy(); }

                // prune shared keys
                index1KeySet.removeAll(otherIndex1KeySet);

                if (index2KeySet != null && otherIndex2KeySet != null) {
                    index2KeySet.removeAll(otherIndex2KeySet);
                }
            }

            // get reverse compliment if necessary
            if (index2KeySet != null && index2ReverseCompliment) {

                HashSet<String> newIndex2KeySet = new HashSet<>();
                for (String key : index2KeySet) {
                    newIndex2KeySet.add(sequenceOperator.getReverseCompliment(key));
                }

                index2KeySet = newIndex2KeySet;
            }

            // add index 1 key -> (sample index spec, index 2 key set) mappings to the lookup dict
            SampleIndexSpecIndex2KeySetPair pair = new SampleIndexSpecIndex2KeySetPair(
                    keyMapping.sampleIndexSpec, index2KeySet
            );

            for (String key : index1KeySet) {
                lookupMap.put(key, pair);
            }
        }
    }

    public SampleIndexSpec lookupSampleIndexSpec(String index1, String index2) {

        SampleIndexSpecIndex2KeySetPair pair = lookupMap.get(index1);

        // if we have not found a pair, then return null
        if (pair == null) { return null; }

        // if we can, then check index 2
        if (index2 != null && pair.index2KeySet != null) {

            if (pair.index2KeySet.contains(index2)) {
                return pair.sampleIndexSpec;
            }
            return null;
        }

        // without index 2 return the sample index spec
        return pair.sampleIndexSpec;
    }
}

class SampleIndexSpecIndex2KeySetPair {

    SampleIndexSpec sampleIndexSpec;
    HashSet<String> index2KeySet;

    SampleIndexSpecIndex2KeySetPair(SampleIndexSpec sampleIndexSpec, HashSet<String> index2KeySet) {
        this.sampleIndexSpec = sampleIndexSpec;
        this.index2KeySet = index2KeySet;
    }
}
