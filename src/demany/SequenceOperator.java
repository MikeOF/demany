package demany;

import java.util.*;

public class SequenceOperator {

    private final Set<Character> validCharacterSet;
    private final Map<Character, Character> complimentByValidCharacter;

    public SequenceOperator(boolean wildcard) {

        // create the valid character set
        validCharacterSet = new HashSet<>(Arrays.asList('A', 'T', 'G', 'C'));

        if (wildcard) { validCharacterSet.add('N'); }

        // create the compliment by valid character set
        complimentByValidCharacter = new HashMap<>();
        complimentByValidCharacter.put('A', 'T');
        complimentByValidCharacter.put('T', 'A');
        complimentByValidCharacter.put('G', 'C');
        complimentByValidCharacter.put('C', 'G');

        if (wildcard) { complimentByValidCharacter.put('N', 'N'); }
    }

    public boolean sequenceCharactersAreValid(String sequence) {

        for (int i = 0; i < sequence.length(); i++) {

            if (!validCharacterSet.contains(sequence.charAt(i))){
                return false;
            }
        }

        return true;
    }

    public String getReverseCompliment(String sequence) {

        StringBuilder reverseComplimentBuilder = new StringBuilder();

        for (int i = sequence.length()-1; i >= 0; i--) {

            reverseComplimentBuilder.append(
                    complimentByValidCharacter.get(
                            sequence.charAt(i)
                    )
            );
        }

        return reverseComplimentBuilder.toString();
    }

    public ArrayList<Character> getValidCharacterList() { return new ArrayList<>(validCharacterSet); }
}
