package demany;

import demany.SampleIndex.SampleIndexSpec;
import org.json.simple.JSONObject;

public class TestUtil {

    public static JSONObject createSampleIndexJSON(String project, String sample, String index1, String index2,
                                            int lane)  {

        JSONObject object = new JSONObject();

        object.put("project", project);
        object.put("sample", sample);
        object.put("index1", index1);
        object.put("index2", index2);
        object.put("lane", lane);

        return object;
    }

    public static SampleIndexSpec getSampleIndexSpec(JSONObject jsonObject) throws Exception {

        return new SampleIndexSpec(jsonObject);
    }

    public static SampleIndexSpec getSampleIndexSpec(String project, String sample, String index1, String index2,
                                              int lane) throws Exception {

        return getSampleIndexSpec(createSampleIndexJSON(project, sample, index1, index2, lane));
    }

}
