package demany.SampleIndex;

import java.util.ArrayList;
import java.util.Set;

public class SampleIndexKeyMappingCollection {

    private final ArrayList<SampleIndexKeyMapping> sampleIndexKeyMappingList;
    private final ArrayList<SampleIndexKeyMappingOverlapCheck> sampleIndexKeyMappingOverlapCheckList;
    private final boolean hasIdentityKeyCollision;
    private final boolean hasKeyOverlap;

    public SampleIndexKeyMappingCollection(Set<SampleIndexSpec> sampleIndexSpecSet, int index1KeyLength,
                                           int index2KeyLength) {

        // validate arguments
        if (sampleIndexSpecSet.isEmpty()) { throw new RuntimeException("the sample index spec set cannot be empty"); }
        if (!(index1KeyLength > 0)) { throw new RuntimeException("index 1 key length must be greater than 0"); }
        if (!(index2KeyLength > -1)) { throw new RuntimeException("index 2 key length must be greater than -1"); }

        // fill the key mapping list
        this.sampleIndexKeyMappingList = new ArrayList<>();
        for (SampleIndexSpec sampleIndexSpec : sampleIndexSpecSet) {

            this.sampleIndexKeyMappingList.add(
                    new SampleIndexKeyMapping(sampleIndexSpec, index1KeyLength, index2KeyLength)
            );
        }

        // create the key mapping overlap check list
        this.sampleIndexKeyMappingOverlapCheckList = new ArrayList<>();
        for (int i = 0; i < this.sampleIndexKeyMappingList.size() - 1; i++) {
            for (int j = i+1; j < this.sampleIndexKeyMappingList.size(); j++) {

                this.sampleIndexKeyMappingOverlapCheckList.add(
                        new SampleIndexKeyMappingOverlapCheck(
                                this.sampleIndexKeyMappingList.get(i), this.sampleIndexKeyMappingList.get(j)
                        )
                );
            }
        }

        // determine if we have an identity key collision anywhere
        this.hasIdentityKeyCollision = this.sampleIndexKeyMappingOverlapCheckList
                .stream()
                .map(SampleIndexKeyMappingOverlapCheck::hasIdentityKeyCollision)
                .reduce(false, (a, b) -> a || b);

        // determine if we have an key overlap anywhere
        this.hasKeyOverlap = this.sampleIndexKeyMappingOverlapCheckList
                .stream()
                .map(SampleIndexKeyMappingOverlapCheck::hasKeyOverlap)
                .reduce(false, (a, b) -> a || b);
    }

    public boolean hasIdentityKeyCollision() {
        return hasIdentityKeyCollision;
    }

    public boolean hasKeyOverlap() {
        return hasKeyOverlap;
    }

    public ArrayList<SampleIndexKeyMapping> getSampleIndexKeyMappingList() {
        return new ArrayList<>(sampleIndexKeyMappingList);
    }

    public ArrayList<String> getOverlapReportLines() {

        ArrayList<String> lineList = new ArrayList<>();

        for (SampleIndexKeyMappingOverlapCheck overlapCheck : sampleIndexKeyMappingOverlapCheckList) {

            lineList.addAll(overlapCheck.getOverlapReportLines());
        }

        return lineList;
    }
}
