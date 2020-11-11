package demany.SampleIndex;

import org.junit.jupiter.api.Test;

import demany.TestUtil;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class SampleIndexKeyMappingCollectionTest {

    @Test
    void testSampleIndexKeyMappingCollectionWithoutCollisionWithoutOverlap() throws Exception {

        HashSet<SampleIndexSpec> sampleIndexSpecSet = new HashSet<>();
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample1", "AGGT", "CGTC", 1
        ));
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample1", "CTAG", "AAGT", 1
        ));

        SampleIndexKeyMappingCollection mappingCollection = new SampleIndexKeyMappingCollection(
                sampleIndexSpecSet, 4, 4
        );
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample3", "ATTG", "GCAC", 1
        ));

        assertFalse(mappingCollection.hasIdentityKeyCollision());
        assertFalse(mappingCollection.hasKeyOverlap());
        assertTrue(mappingCollection.getOverlapReportLines().isEmpty());
    }

    @Test
    void testSampleIndexKeyMappingCollectionWithoutCollisionWithOverlap() throws Exception {

        HashSet<SampleIndexSpec> sampleIndexSpecSet = new HashSet<>();
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample1", "CACG", "CGTC", 1
        ));
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample1", "CTAG", "AAGT", 1
        ));
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample3", "ATTG", "GCAC", 1
        ));

        SampleIndexKeyMappingCollection mappingCollection = new SampleIndexKeyMappingCollection(
                sampleIndexSpecSet, 4, 4
        );

        assertFalse(mappingCollection.hasIdentityKeyCollision());
        assertTrue(mappingCollection.hasKeyOverlap());
        assertFalse(mappingCollection.getOverlapReportLines().isEmpty());
    }

    @Test
    void testSampleIndexKeyMappingCollectionWithCollisionWithOverlap() throws Exception {

        HashSet<SampleIndexSpec> sampleIndexSpecSet = new HashSet<>();
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample1", "CACG", "CGTC", 1
        ));
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample2", "CTAG", "CGTC", 1
        ));
        sampleIndexSpecSet.add(TestUtil.getSampleIndexSpec(
                "TestProject1", "TestSample3", "ATTG", "GCAC", 1
        ));

        SampleIndexKeyMappingCollection mappingCollection = new SampleIndexKeyMappingCollection(
                sampleIndexSpecSet, 4, 4
        );

        assertTrue(mappingCollection.hasIdentityKeyCollision());
        assertTrue(mappingCollection.hasKeyOverlap());
        assertFalse(mappingCollection.getOverlapReportLines().isEmpty());
    }
}