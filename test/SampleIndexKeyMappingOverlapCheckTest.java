import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleIndexKeyMappingOverlapCheckTest {

    private SampleIndexKeyMappingOverlapCheck getSampleIndexKeyMappingOverlapCheck(
            String firstIndex1, String firstIndex2, String secondIndex1, String secondIndex2) throws Exception {

        return new SampleIndexKeyMappingOverlapCheck(
                new SampleIndexKeyMapping(
                        SampleIndexSpecTestHelper.getSampleIndexSpec(
                                "TestProject1", "TestSample1", firstIndex1, firstIndex2, 1
                        ),
                        3,
                        3
                ),
                new SampleIndexKeyMapping(
                        SampleIndexSpecTestHelper.getSampleIndexSpec(
                                "TestProject2", "TestSample2", secondIndex1, secondIndex2, 1
                        ),
                        3,
                        3
                )
        );
    }

    @Test
    public void testSampleIndexKeyMappingOverlapCheckNoOverlapNoCollision() throws Exception {

        SampleIndexKeyMappingOverlapCheck overlapCheck;

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "AGC", "TCG","TCG", "CTA"
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertFalse(overlapCheck.hasKeyOverlap());
        assertTrue(overlapCheck.hasIndex2KeyOverlapSets());

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "AGC", null,"TCG", "CTA"
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertFalse(overlapCheck.hasKeyOverlap());
        assertFalse(overlapCheck.hasIndex2KeyOverlapSets());

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "AGC", "TTT","TCG", null
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertFalse(overlapCheck.hasKeyOverlap());
        assertFalse(overlapCheck.hasIndex2KeyOverlapSets());

    }

    @Test
    public void testSampleIndexKeyMappingOverlapCheckOverlapNoCollision() throws Exception {

        SampleIndexKeyMappingOverlapCheck overlapCheck;

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "AGG", "TCG","TCG", "CTA"
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertTrue(overlapCheck.hasKeyOverlap());
        assertTrue(overlapCheck.hasIndex2KeyOverlapSets());

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "AGG", "TCG","TCA", "CTG"
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertTrue(overlapCheck.hasKeyOverlap());
        assertTrue(overlapCheck.hasIndex2KeyOverlapSets());

        overlapCheck = getSampleIndexKeyMappingOverlapCheck(
                "TGG", null,"TCA", null
        );

        assertFalse(overlapCheck.hasIdentityKeyCollision());
        assertTrue(overlapCheck.hasKeyOverlap());
        assertFalse(overlapCheck.hasIndex2KeyOverlapSets());
    }
}
