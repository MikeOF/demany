package demany.Fastq;

import java.util.*;

public class SequenceOperator {

    private static final Set<Character> validCharacterSet = Set.of('A', 'T', 'G', 'C', 'N');;
    private static final Map<Character, Character> complimentByValidCharacter = Map.of(
            'A', 'T',
            'T', 'A',
            'G', 'C',
            'C', 'G',
            'N', 'N'
    );

    public static boolean sequenceCharactersAreValid(String sequence) {

        for (int i = 0; i < sequence.length(); i++) {

            if (!validCharacterSet.contains(sequence.charAt(i))){
                return false;
            }
        }

        return true;
    }

    public static String getReverseCompliment(String sequence) {

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

    public static ArrayList<Character> getValidCharacterList() { return new ArrayList<>(validCharacterSet); }
}
