package demany.SampleIndex;

import java.util.ArrayList;
import java.util.HashSet;

public class SampleIndexKeyMappingOverlapCheck {

    public final SampleIndexKeyMapping firstSampleIndexKeyMapping;
    public final SampleIndexKeyMapping secondSampleIndexKeyMapping;
    private final HashSet<String> index1KeyOverlapSet;
    private final HashSet<String> index1IdentityKeyOverlapSet;
    private final HashSet<String> index2KeyOverlapSet;
    private final HashSet<String> index2IdentityKeyOverlapSet;

    public SampleIndexKeyMappingOverlapCheck(SampleIndexKeyMapping firstSampleIndexKeyMapping,
                                             SampleIndexKeyMapping secondSampleIndexKeyMapping) {

        this.firstSampleIndexKeyMapping = firstSampleIndexKeyMapping;
        this.secondSampleIndexKeyMapping = secondSampleIndexKeyMapping;

        // get index overlap sets
        this.index1KeyOverlapSet = getOverlapSet(
                firstSampleIndexKeyMapping.getIndex1KeySetCopy(),
                secondSampleIndexKeyMapping.getIndex1KeySetCopy()
        );

        this.index1IdentityKeyOverlapSet = getOverlapSet(
                firstSampleIndexKeyMapping.getIndex1IdentityKeySetCopy(),
                secondSampleIndexKeyMapping.getIndex1IdentityKeySetCopy()
        );

        // get index 2 overlap sets if possible
        if (firstSampleIndexKeyMapping.hasIndex2() && secondSampleIndexKeyMapping.hasIndex2()) {

            this.index2KeyOverlapSet = getOverlapSet(
                    firstSampleIndexKeyMapping.getIndex2KeySetCopy(),
                    secondSampleIndexKeyMapping.getIndex2KeySetCopy()
            );

            this.index2IdentityKeyOverlapSet = getOverlapSet(
                    firstSampleIndexKeyMapping.getIndex2IdentityKeySetCopy(),
                    secondSampleIndexKeyMapping.getIndex2IdentityKeySetCopy()
            );
        } else {
            this.index2KeyOverlapSet = null;
            this.index2IdentityKeyOverlapSet = null;
        }

    }

    private static HashSet<String> getOverlapSet(HashSet<String> firstSet, HashSet<String> secondSet) {

        HashSet<String> overlapSet = new HashSet<>(firstSet);
        overlapSet.retainAll(secondSet);

        return overlapSet;
    }

    public boolean hasIndex2KeyOverlapSets() {
        return index2KeyOverlapSet != null;
    }

    private boolean hasIndex1IdentityKeyCollision() {
        return !index1IdentityKeyOverlapSet.isEmpty();
    }

    private boolean hasIndex2IdentityKeyCollision() {
        return hasIndex2KeyOverlapSets() && !index2IdentityKeyOverlapSet.isEmpty();
    }

    public boolean hasIdentityKeyCollision() {
        return hasIndex1IdentityKeyCollision() || hasIndex2IdentityKeyCollision();
    }

    private boolean hasIndex1KeyOverlap() {
        return !index1KeyOverlapSet.isEmpty();
    }

    private boolean hasIndex2KeyOverlap() {
        return hasIndex2KeyOverlapSets() && !index2KeyOverlapSet.isEmpty();
    }

    public boolean hasKeyOverlap() {
        return hasIndex1KeyOverlap() || hasIndex2KeyOverlap();
    }

    public ArrayList<String> getOverlapReportLines() {

        ArrayList<String> lineList = new ArrayList<>();
        StringBuilder lineBuilder;

        // first and second index strings
        String firstAndSecondIndex1 = String.format(
                "first: %s, second: %s",
                firstSampleIndexKeyMapping.sampleIndexSpec.index1,
                secondSampleIndexKeyMapping.sampleIndexSpec.index1
        );
        String firstAndSecondIndex2 = String.format(
                "first: %s, second: %s",
                firstSampleIndexKeyMapping.sampleIndexSpec.index2,
                secondSampleIndexKeyMapping.sampleIndexSpec.index2
        );

        // lines for identity key collision
        if (hasIdentityKeyCollision()) {

            // line for index 1 collision
            if (hasIndex1IdentityKeyCollision()) {

                lineBuilder = new StringBuilder();

                lineBuilder.append(
                        String.format("%s - %s and %s - %s have a collision on index 1, ",
                                firstSampleIndexKeyMapping.sampleIndexSpec.project,
                                firstSampleIndexKeyMapping.sampleIndexSpec.sample,
                                secondSampleIndexKeyMapping.sampleIndexSpec.project,
                                secondSampleIndexKeyMapping.sampleIndexSpec.sample
                        )
                );

                lineBuilder.append(firstAndSecondIndex1);

                lineList.add(lineBuilder.toString());
            }

            // line for index 2 collision
            if (hasIndex2IdentityKeyCollision()) {

                lineBuilder = new StringBuilder();

                lineBuilder.append(
                        String.format("%s - %s and %s - %s have a collision on index 2, ",
                                firstSampleIndexKeyMapping.sampleIndexSpec.project,
                                firstSampleIndexKeyMapping.sampleIndexSpec.sample,
                                secondSampleIndexKeyMapping.sampleIndexSpec.project,
                                secondSampleIndexKeyMapping.sampleIndexSpec.sample
                        )
                );

                lineBuilder.append(firstAndSecondIndex2);

                lineList.add(lineBuilder.toString());
            }
        }

        // lines for key overlap
        if (hasKeyOverlap()) {

            // line for index 1 key overlap
            if (hasIndex1KeyOverlap()) {

                lineBuilder = new StringBuilder();

                lineBuilder.append(
                        String.format("%s - %s and %s - %s have %d shared keys for index 1, ",
                                firstSampleIndexKeyMapping.sampleIndexSpec.project,
                                firstSampleIndexKeyMapping.sampleIndexSpec.sample,
                                secondSampleIndexKeyMapping.sampleIndexSpec.project,
                                secondSampleIndexKeyMapping.sampleIndexSpec.sample,
                                index1KeyOverlapSet.size()
                        )
                );

                lineBuilder.append(firstAndSecondIndex1);

                lineList.add(lineBuilder.toString());
            }

            // line fore index 2 key overlap
            if (hasIndex2KeyOverlap()) {

                lineBuilder = new StringBuilder();

                lineBuilder.append(
                        String.format("%s - %s and %s - %s have %d shared keys for index 2, ",
                                firstSampleIndexKeyMapping.sampleIndexSpec.project,
                                firstSampleIndexKeyMapping.sampleIndexSpec.sample,
                                secondSampleIndexKeyMapping.sampleIndexSpec.project,
                                secondSampleIndexKeyMapping.sampleIndexSpec.sample,
                                index2KeyOverlapSet.size()
                        )
                );

                lineBuilder.append(firstAndSecondIndex2);

                lineList.add(lineBuilder.toString());
            }

        }

        return lineList;
    }

}
