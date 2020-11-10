import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SampleIndexSpecTestHelper {

    protected static JSONObject createSampleIndexJSON(String project, String sample, String index1, String index2,
                                                      int lane)  {

        JSONObject object = new JSONObject();

        object.put("project", project);
        object.put("sample", sample);
        object.put("index1", index1);
        object.put("index2", index2);
        object.put("lane", lane);

        return object;
    }

    protected static SampleIndexSpec getSampleIndexSpec(JSONObject jsonObject) throws Exception {

        return new SampleIndexSpec(jsonObject);
    }

    protected static SampleIndexSpec getSampleIndexSpec(String project, String sample, String index1, String index2,
                                                        int lane) throws Exception {

        return getSampleIndexSpec(createSampleIndexJSON(project, sample, index1, index2, lane));
    }
}

class SampleIndexSpecTest {

    @Test
    void testSampleIndexSpecConstructionWithIndex2() throws Exception {

        SampleIndexSpec sampleIndexSpec = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecEquals = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent1 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSampleD", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent2 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProjectD", "TestSample", "AGGTC", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent3 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTCA", "TCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent4 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent5 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "ATCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent6 = SampleIndexSpecTestHelper.getSampleIndexSpec(
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

        SampleIndexSpec sampleIndexSpec = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecEquals = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent1 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSampleD", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent2 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProjectD", "TestSample", "AGGTC", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent3 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTCA", null, 1
        );

        SampleIndexSpec sampleIndexSpecDifferent4 = SampleIndexSpecTestHelper.getSampleIndexSpec(
                "TestProject", "TestSample", "AGGTC", "ATCGTT", 1
        );

        SampleIndexSpec sampleIndexSpecDifferent5 = SampleIndexSpecTestHelper.getSampleIndexSpec(
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
