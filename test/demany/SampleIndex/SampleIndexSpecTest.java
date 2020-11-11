package demany.SampleIndex;

import demany.TestUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SampleIndexSpecTest {

    @Test
    void testSampleIndexSpecConstructionWithIndex2() throws Exception {

        SampleIndexSpec sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecEquals = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent1 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSampleD", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent2 = TestUtil.getSampleIndexSpec(
                "TestProjectD", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent3 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTCA", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent4 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent5 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "ATCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent6 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "TCGTT", 2
        );

        assertEquals(sampleIndexSpec, sampleIndexSpecEquals);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent1);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent2);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent3);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent4);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent5);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent6);
        assertEquals(sampleIndexSpec.hashCode(), sampleIndexSpecEquals.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent1.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent2.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent3.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent4.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent5.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent6.hashCode());
    }

    @Test
    void testSampleIndexSpecConstructionWithoutIndex2() throws Exception {

        SampleIndexSpec sampleIndexSpec = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecEquals = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent1 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSampleD", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent2 = TestUtil.getSampleIndexSpec(
                "TestProjectD", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent3 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTCA", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent4 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "ATCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent5 = TestUtil.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 2
        );

        assertEquals(sampleIndexSpec, sampleIndexSpecEquals);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent1);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent2);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent3);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent4);
        assertNotEquals(sampleIndexSpec, sampleIndexSpecDifferent5);
        assertEquals(sampleIndexSpec.hashCode(), sampleIndexSpecEquals.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent1.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent2.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent3.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent4.hashCode());
        assertNotEquals(sampleIndexSpec.hashCode(), sampleIndexSpecDifferent5.hashCode());

    }
}
