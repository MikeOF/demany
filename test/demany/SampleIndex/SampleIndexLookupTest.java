package demany.SampleIndex;

import demany.TestUtil;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class SampleIndexLookupTest {

    private static SampleIndexLookup createSampleIndexLookup(
            String[][] argArrayArray, int index1KeyLength,int index2KeyLength, boolean index2ReverseCompliment
    ) throws Exception {

        HashSet<SampleIndexSpec> specSet = new HashSet<>();
        for (String[] argArray : argArrayArray) {
            specSet.add(TestUtil.getSampleIndexSpec(
                    argArray[0], argArray[1], argArray[2], argArray[3], Integer.parseInt(argArray[4])
            ));
        }

        SampleIndexKeyMappingCollection mappingCollection = new SampleIndexKeyMappingCollection(
                specSet, index1KeyLength, index2KeyLength
        );

        return new SampleIndexLookup(mappingCollection, index2ReverseCompliment);
    }

    @Test
    void testLookupWithoutOverlap() throws Exception {

        String[][] specArrayArray = {
                {"TestProject1", "TestSample1", "AGCT", "CCGT", "1"},
                {"TestProject2", "TestSample2", "CAGG", "GTAA", "1"},
                {"TestProject3", "TestSample3", "TCAC", "AACC", "1"}
        };
        SampleIndexLookup lookup = createSampleIndexLookup(
                specArrayArray,
                4, 4, false
        );

        assertEquals("TestProject1", lookup.lookupSampleIndexSpec("AGCT", "CCGT").project);
        assertEquals("TestSample2", lookup.lookupSampleIndexSpec("CAGG", "GTAA").sample);
        assertEquals("TCAC", lookup.lookupSampleIndexSpec("TCAC", "AACC").index1);
        assertEquals("TestProject1", lookup.lookupSampleIndexSpec("AGCG", "CCGT").project);
        assertEquals("TestSample2", lookup.lookupSampleIndexSpec("CAGG", "GTAT").sample);
        assertEquals("TCAC", lookup.lookupSampleIndexSpec("TNAC", "ANCC").index1);
        assertNull(lookup.lookupSampleIndexSpec("ATCG", "ACGT"));
        assertNull(lookup.lookupSampleIndexSpec("CTTG", "GTAA"));
        assertNull(lookup.lookupSampleIndexSpec("NNAC", "AACC"));
    }

    @Test
    void testLookupWithOverlap() throws Exception {

        String[][] specArrayArray = {
                {"TestProject1", "TestSample1", "AGCT", "CCGT", "1"},
                {"TestProject2", "TestSample2", "ACGT", "GTAA", "1"},
                {"TestProject3", "TestSample3", "TCAC", "AACC", "1"}
        };
        SampleIndexLookup lookup = createSampleIndexLookup(
                specArrayArray,
                4, 4, false
        );

        // Identity lookup
        assertEquals("TestProject1", lookup.lookupSampleIndexSpec("AGCT", "CCGT").project);
        assertEquals("TestSample2", lookup.lookupSampleIndexSpec("ACGT", "GTAA").sample);
        assertEquals("TCAC", lookup.lookupSampleIndexSpec("TCAC", "AACC").index1);

        // Pruned Key miss
        assertNull(lookup.lookupSampleIndexSpec("ACCT", "CCGT"));
        assertNull(lookup.lookupSampleIndexSpec("AGGT", "GTAA"));

        // Key lookup
        assertEquals("TestProject1", lookup.lookupSampleIndexSpec("AGCG", "CCGT").project);
        assertEquals("TestSample2", lookup.lookupSampleIndexSpec("ACGT", "GTAT").sample);
        assertEquals("TCAC", lookup.lookupSampleIndexSpec("TNAC", "ANCC").index1);

        // Nonsense misses
        assertNull(lookup.lookupSampleIndexSpec("ATCG", "ACGT"));
        assertNull(lookup.lookupSampleIndexSpec("CTTG", "GTAA"));
        assertNull(lookup.lookupSampleIndexSpec("NNAC", "AACC"));
    }

    @Test
    void testLookupWithCollision() throws Exception {

        String[][] specArrayArray1 = {
                {"TestProject1", "TestSample1", "AGCT", "CCGT", "1"},
                {"TestProject2", "TestSample2", "AGCT", "GTAA", "1"},
                {"TestProject3", "TestSample3", "TCAC", "AACC", "1"}
        };

        // Throws exceptions
        assertThrows(
            RuntimeException.class,
            () -> {
                createSampleIndexLookup(
                        specArrayArray1, 4, 4, false
                );
            }
        );

        String[][] specArrayArray2 = {
                {"TestProject1", "TestSample1", "AGCT", "CCGT", "1"},
                {"TestProject2", "TestSample2", "ACTC", "GTAA", "1"},
                {"TestProject3", "TestSample3", "TCAC", "GTAA", "1"}
        };

        // Throws exceptions
        assertThrows(
                RuntimeException.class,
                () -> {
                    createSampleIndexLookup(
                            specArrayArray2, 4, 4, false
                    );
                }
        );
    }
}