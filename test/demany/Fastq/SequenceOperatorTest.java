package demany.Fastq;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SequenceOperatorTest {

    @Test
    void sequenceCharactersAreValid() {

        assertTrue(SequenceOperator.sequenceCharactersAreValid("ACGTTGCGCGTA"));
        assertFalse(SequenceOperator.sequenceCharactersAreValid("ACFGD"));
        assertTrue(SequenceOperator.sequenceCharactersAreValid("ACN"));
    }

    @Test
    void getReverseCompliment() {

        assertEquals("TNTACGCN", SequenceOperator.getReverseCompliment("NGCGTANA"));

    }
}