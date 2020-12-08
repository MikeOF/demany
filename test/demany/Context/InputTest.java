package demany.Context;

import demany.SampleIndex.SampleIndexSpec;
import demany.TestUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class InputTestHelper {

    static JSONObject createInputJSON() {

        JSONObject object = new JSONObject();
        JSONArray array = new JSONArray();

        object.put("processingThreadNumber", "4");
        object.put("workdirPath", "test/workdir");
        object.put("sampleIndexSpecArray", array);
        object.put("bclPath", "test/bcl");

        return object;
    }

    static void addSampleIndexJSON(JSONObject input, JSONObject jsonObject) {

        JSONArray sampleIndexSpecArray = (JSONArray) input.get("sampleIndexSpecArray");

        sampleIndexSpecArray.add(jsonObject);
    }

}

class InputTest {

    @Test
    void testInputObjectConstructorDemultiplex() throws Exception {

        JSONObject inputObject = InputTestHelper.createInputJSON();
        JSONObject sampleIndexJSON = TestUtil.createSampleIndexJSON(
                "TestProject", "TestSample", "AGGGC", "TCGAA",  2
        );
        InputTestHelper.addSampleIndexJSON(inputObject, sampleIndexJSON);
        SampleIndexSpec sampleIndexSpec = new SampleIndexSpec(
                "TestProject-TestSample",
                "TestProject",
                "TestSample",
                "AGGGC",
                "TCGAA",
                2
        );

        Input input = new Input(inputObject.toJSONString());

        assertEquals(4, input.processingThreadNumber);
        assertEquals(sampleIndexSpec, input.sampleIndexSpecSet.iterator().next());
        assertEquals(Path.of("test/workdir").toAbsolutePath(), input.workdirPath);
        assertEquals(Path.of("test/bcl").toAbsolutePath(), input.bclPath);
        assertNull(input.useBasesMaskArg);
    }

    @Test
    void testInputObjectConstructorDemultiplexWithUseBasesMaskArg() throws Exception {

        JSONObject inputObject = InputTestHelper.createInputJSON();
        inputObject.put("--use-bases-mask", "Y51,I8,Y16,Y51");
        JSONObject sampleIndexJSON = TestUtil.createSampleIndexJSON(
                "TestProject", "TestSample", "AGGGC", "TCGAA",  2
        );
        InputTestHelper.addSampleIndexJSON(inputObject, sampleIndexJSON);
        SampleIndexSpec sampleIndexSpec = new SampleIndexSpec(
                "TestProject-TestSample",
                "TestProject",
                "TestSample",
                "AGGGC",
                "TCGAA",
                2
        );

        Input input = new Input(inputObject.toJSONString());

        assertEquals(4, input.processingThreadNumber);
        assertEquals(sampleIndexSpec, input.sampleIndexSpecSet.iterator().next());
        assertEquals(Path.of("test/workdir").toAbsolutePath(), input.workdirPath);
        assertEquals(Path.of("test/bcl").toAbsolutePath(), input.bclPath);
        assertEquals("Y51,I8,Y16,Y51", input.useBasesMaskArg);
    }
}
