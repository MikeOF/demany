package demany.SampleIndex;

import org.junit.jupiter.api.Test;
import demany.TestUtil;

import static org.junit.jupiter.api.Assertions.*;

class SampleIndexKeyMappingTest {

    @Test
    void testIdentityKeySets() throws Exception {

        SampleIndexSpec sampleIndexSpec;
        SampleIndexKeyMapping keyMapping;

        // key length equals index length
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 3, 3
        );

        assertEquals(1, keyMapping.getIndex1IdentityKeySetCopy().size());
        assertEquals(1, keyMapping.getIndex2IdentityKeySetCopy().size());
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AGG"));
        assertTrue(keyMapping.getIndex2IdentityKeySetCopy().contains("TTA"));

        // key length is less than the index length
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 2, 2
        );

        assertEquals(1, keyMapping.getIndex1IdentityKeySetCopy().size());
        assertEquals(1, keyMapping.getIndex2IdentityKeySetCopy().size());
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AG"));
        assertTrue(keyMapping.getIndex2IdentityKeySetCopy().contains("TT"));

        // key length is greater than the index length
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 4, 4
        );

        assertEquals(5, keyMapping.getIndex1IdentityKeySetCopy().size());
        assertEquals(5, keyMapping.getIndex2IdentityKeySetCopy().size());
        assertFalse(keyMapping.getIndex1IdentityKeySetCopy().contains("AGG"));
        assertFalse(keyMapping.getIndex1IdentityKeySetCopy().contains("AGTT"));
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AGGN"));
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AGGT"));
        assertFalse(keyMapping.getIndex2IdentityKeySetCopy().contains("TTA"));
        assertFalse(keyMapping.getIndex2IdentityKeySetCopy().contains("TCAG"));
        assertTrue(keyMapping.getIndex2IdentityKeySetCopy().contains("TTAN"));
        assertTrue(keyMapping.getIndex2IdentityKeySetCopy().contains("TTAT"));

        // key lengths are different
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 4, 3
        );

        assertEquals(5, keyMapping.getIndex1IdentityKeySetCopy().size());
        assertEquals(1, keyMapping.getIndex2IdentityKeySetCopy().size());
        assertFalse(keyMapping.getIndex1IdentityKeySetCopy().contains("AGG"));
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AGGN"));
        assertTrue(keyMapping.getIndex1IdentityKeySetCopy().contains("AGGT"));
        assertTrue(keyMapping.getIndex2IdentityKeySetCopy().contains("TTA"));
        assertFalse(keyMapping.getIndex2IdentityKeySetCopy().contains("TTT"));
    }

    @Test
    void testKeySets() throws Exception {

        SampleIndexSpec sampleIndexSpec;
        SampleIndexKeyMapping keyMapping;

        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGT", "CTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 3, 3
        );

        assertEquals(13, keyMapping.getIndex1KeySetCopy().size());
        assertEquals(13, keyMapping.getIndex1KeySetCopy().size());
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AGT"));
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AGC"));
        assertFalse(keyMapping.getIndex1KeySetCopy().contains("TAT"));
        assertFalse(keyMapping.getIndex1KeySetCopy().contains("NGC"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("CTA"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("CTN"));
        assertFalse(keyMapping.getIndex2KeySetCopy().contains("CGT"));
        assertFalse(keyMapping.getIndex2KeySetCopy().contains("CAN"));

        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 2, 2
        );

        assertEquals(9, keyMapping.getIndex1KeySetCopy().size());
        assertEquals(9, keyMapping.getIndex2KeySetCopy().size());
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AG"));
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AT"));
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("NG"));
        assertFalse(keyMapping.getIndex1KeySetCopy().contains("TT"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("TT"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("TN"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("AT"));
        assertFalse(keyMapping.getIndex2KeySetCopy().contains("GC"));

        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGT", "CTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 4, 4
        );

        assertEquals(65, keyMapping.getIndex1KeySetCopy().size());
        assertEquals(65, keyMapping.getIndex1KeySetCopy().size());
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AGTC"));
        assertTrue(keyMapping.getIndex1KeySetCopy().contains("AGCN"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("CTGN"));
        assertTrue(keyMapping.getIndex2KeySetCopy().contains("CTAG"));
    }


    @Test
    void testHasIndex2() throws Exception {

        SampleIndexSpec sampleIndexSpec;
        SampleIndexKeyMapping keyMapping;

        // false index key length == 0
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 3, 0
        );

        assertFalse(keyMapping.hasIndex2());

        // false index 2 == null
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", null, 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 3, 3
        );

        assertFalse(keyMapping.hasIndex2());

        // true
        sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGG", "TTA", 2
        );
        keyMapping = new SampleIndexKeyMapping(
                sampleIndexSpec, 3, 3
        );

        assertTrue(keyMapping.hasIndex2());
    }

}