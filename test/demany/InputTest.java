package demany;

import demany.SampleIndex.SampleIndexSpec;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class InputTestHelper {

    static JSONObject createInputJSON(String program) {

        JSONObject object = new JSONObject();
        JSONArray array = new JSONArray();

        object.put("program", program);
        object.put("workdirPath", "test/workdir");
        object.put("sampleIndexSpecArray", array);

        if (program.equals(Input.Program.DEMULTIPLEX.toString())) {
            object.put("bclPath", "test/bcl");
        }

        return object;
    }

    static void addSampleIndexJSON(JSONObject input, JSONObject jsonObject) {

        JSONArray sampleIndexSpecArray = (JSONArray) input.get("sampleIndexSpecArray");

        sampleIndexSpecArray.add(jsonObject);
    }

}

class InputTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void testInputObjectConstructorCheckIndices() throws Exception {

        JSONObject inputObject = InputTestHelper.createInputJSON("CHECK_INDICES");
        JSONObject sampleIndexJSON = TestUtil.createSampleIndexJSON(
                "TestProject", "TestSample", "AGGGC", "TCGAA",  2
        );
        InputTestHelper.addSampleIndexJSON(inputObject, sampleIndexJSON);
        SampleIndexSpec sampleIndexSpec = TestUtil.getSampleIndexSpec(sampleIndexJSON);

        Input input = new Input(inputObject.toJSONString());

        assertEquals("CHECK_INDICES", input.program.name());
        assertEquals(sampleIndexSpec, input.sampleIndexSpecSet.iterator().next());
    }
}
