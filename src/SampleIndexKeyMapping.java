import java.util.ArrayList;
import java.util.HashSet;

public class SampleIndexKeyMapping {

    public final SampleIndexSpec sampleIndexSpec;
    private final HashSet<String> index1IdentityKeySet;
    private final HashSet<String> index2IdentityKeySet;
    private final HashSet<String> index1KeySet;
    private final HashSet<String> index2KeySet;

    public SampleIndexKeyMapping(SampleIndexSpec sampleIndexSpec, int index1KeyLength, int index2KeyLength) throws RuntimeException {

        // validate arguments
        if (!(index1KeyLength > 0)) { throw new RuntimeException("index 1 key length must be greater than 0"); }
        if (!(index2KeyLength > -1)) { throw new RuntimeException("index 2 key length must be greater than -1"); }

        this.sampleIndexSpec = sampleIndexSpec;

        // index 1 key set
        this.index1IdentityKeySet = new HashSet<>();
        this.index1KeySet = new HashSet<>();
        fillKeySet(sampleIndexSpec.index1, index1KeyLength, index1IdentityKeySet, index1KeySet);

        // index 2 key set
        if (index2KeyLength == 0 || sampleIndexSpec.index2 == null) {
            this.index2IdentityKeySet = null;
            this.index2KeySet = null;

        } else {
            this.index2IdentityKeySet = new HashSet<>();
            this.index2KeySet = new HashSet<>();

            fillKeySet(sampleIndexSpec.index2, index2KeyLength, index2IdentityKeySet, index2KeySet);
        }

    }

    public boolean hasIndex2() {
        return index2KeySet != null;
    }

    public HashSet<String> getIndex1IdentityKeySetCopy() {
        return new HashSet<>(index1IdentityKeySet);
    }

    public HashSet<String> getIndex1KeySetCopy() {
        return new HashSet<>(index1KeySet);
    }

    public HashSet<String> getIndex2IdentityKeySetCopy() {
        return new HashSet<>(index2IdentityKeySet);
    }

    public HashSet<String> getIndex2KeySetCopy() {
        return new HashSet<>(index2KeySet);
    }

    private static void fillKeySet(String index, int keyLength, HashSet<String> identityKeySet, HashSet<String> keySet) {

        // get a sequence operator
        SequenceOperator sequenceOperator = new SequenceOperator(true);

        for (int variableIndex = -1; variableIndex < Math.min(index.length(), keyLength); variableIndex++) {


            // get a list with lists of characters at each position
            ArrayList<ArrayList<Character>> positionList = new ArrayList<>();
            for (int positionIndex = 0; positionIndex < keyLength; positionIndex++) {

                if (positionIndex == variableIndex || positionIndex >= index.length()) {

                    // add a list of all valid characters
                    positionList.add(sequenceOperator.getValidCharacterList());

                } else {

                    // add a list with just the character at the position in the index
                    ArrayList<Character> indexCharList = new ArrayList<>();
                    indexCharList.add(index.charAt(positionIndex));

                    positionList.add(indexCharList);
                }
            }

            // create keys from the position list
            HashSet<String> buildKeySet = new HashSet<>();
            buildKeySet.add("");
            for (ArrayList<Character> characterList: positionList) {

                HashSet<String> newBuildKeySet = new HashSet<>();

                for (String buildKey : buildKeySet) {

                    for (Character character : characterList) {

                        newBuildKeySet.add(buildKey + character);
                    }
                }

                buildKeySet = newBuildKeySet;
            }

            // add the newly build keys to the key set
            keySet.addAll(buildKeySet);

            // if the variable index is -1, we have our identity key set
            if (variableIndex == -1) { identityKeySet.addAll(buildKeySet); }
        }
    }

    @Override
    public boolean equals(Object object) {

        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        SampleIndexKeyMapping other = (SampleIndexKeyMapping) object;

        return sampleIndexSpec.equals(other.sampleIndexSpec);
    }

    @Override
    public int hashCode() {
        return sampleIndexSpec.hashCode();
    }
}
