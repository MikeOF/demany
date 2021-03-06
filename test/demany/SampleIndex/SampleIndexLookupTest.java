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

        assertEquals("TestProject1-TestSample1", lookup.lookupProjectSampleId("AGCT", "CCGT"));
        assertEquals("TestProject2-TestSample2", lookup.lookupProjectSampleId("CAGG", "GTAA"));
        assertEquals("TestProject3-TestSample3", lookup.lookupProjectSampleId("TCAC", "AACC"));
        assertEquals("TestProject1-TestSample1", lookup.lookupProjectSampleId("AGCG", "CCGT"));
        assertEquals("TestProject2-TestSample2", lookup.lookupProjectSampleId("CAGG", "GTAT"));
        assertEquals("TestProject3-TestSample3", lookup.lookupProjectSampleId("TNAC", "ANCC"));
        assertNull(lookup.lookupProjectSampleId("ATCG", "ACGT"));
        assertNull(lookup.lookupProjectSampleId("CTTG", "GTAA"));
        assertNull(lookup.lookupProjectSampleId("NNAC", "AACC"));
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
        assertEquals("TestProject1-TestSample1", lookup.lookupProjectSampleId("AGCT", "CCGT"));
        assertEquals("TestProject2-TestSample2", lookup.lookupProjectSampleId("ACGT", "GTAA"));
        assertEquals("TestProject3-TestSample3", lookup.lookupProjectSampleId("TCAC", "AACC"));

        // Pruned Key miss
        assertNull(lookup.lookupProjectSampleId("ACCT", "CCGT"));
        assertNull(lookup.lookupProjectSampleId("AGGT", "GTAA"));

        // Key lookup
        assertEquals("TestProject1-TestSample1", lookup.lookupProjectSampleId("AGCG", "CCGT"));
        assertEquals("TestProject2-TestSample2", lookup.lookupProjectSampleId("ACGT", "GTAT"));
        assertEquals("TestProject3-TestSample3", lookup.lookupProjectSampleId("TNAC", "ANCC"));

        // Nonsense misses
        assertNull(lookup.lookupProjectSampleId("ATCG", "ACGT"));
        assertNull(lookup.lookupProjectSampleId("CTTG", "GTAA"));
        assertNull(lookup.lookupProjectSampleId("NNAC", "AACC"));
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