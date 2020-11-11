package demany.Utils;

import demany.Utils.SequenceOperator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SequenceOperatorTest {

    @Test
    void sequenceCharactersAreValidWithoutWildcard() {

        SequenceOperator seqOperator = new SequenceOperator(false);

        assertTrue(seqOperator.sequenceCharactersAreValid("ACGTTGCGCGTA"));
        assertFalse(seqOperator.sequenceCharactersAreValid("ACFGD"));
        assertFalse(seqOperator.sequenceCharactersAreValid("ACN"));
    }

    @Test
    void sequenceCharactersAreValidWithWildcard() {

        SequenceOperator seqOperator = new SequenceOperator(true);

        assertTrue(seqOperator.sequenceCharactersAreValid("ACGTTGCGCGTA"));
        assertFalse(seqOperator.sequenceCharactersAreValid("ACFGD"));
        assertTrue(seqOperator.sequenceCharactersAreValid("ACN"));
    }

    @Test
    void getReverseComplimentWithoutWildcard() {

        SequenceOperator seqOperator = new SequenceOperator(false);

        assertEquals("AACGT", seqOperator.getReverseCompliment("ACGTT"));
    }

    @Test
    void getReverseComplimentWithWildcard() {

        SequenceOperator seqOperator = new SequenceOperator(true);

        assertEquals("TNTACGCN", seqOperator.getReverseCompliment("NGCGTANA"));

    }
}