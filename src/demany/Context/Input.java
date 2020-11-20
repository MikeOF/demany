package demany.Context;

import demany.SampleIndex.SampleIndexSpec;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Input {

    // key values
    static final String sampleIndexSpecArrayKey = "sampleIndexSpecArray";
    static final String workdirPathKey = "workdirPath";
    static final String bclPathKey = "bclPath";
    static final String processingThreadNumberKey = "processingThreadNumber";

    public final Set<SampleIndexSpec> sampleIndexSpecSet;
    public final boolean sampleSpecSetHasIndex2;
    public final Path workdirPath;
    public final Path bclPath;
    public final int processingThreadNumber;

    public Input(String jsonInput) throws Exception {

        // parse the input JSON
        JSONObject inputObject = (JSONObject) JSONValue.parse(jsonInput);

        // create the sample index spec array
        JSONArray sampleIndexSpecJSONArray = (JSONArray) inputObject.get(Input.sampleIndexSpecArrayKey);

        // collect sample index specs
        Set<SampleIndexSpec> tempSampleIndexSpecSet = new HashSet<>();
        for (Object sampleIndexSpecObject : sampleIndexSpecJSONArray) {

            tempSampleIndexSpecSet.add(new SampleIndexSpec((JSONObject) sampleIndexSpecObject));
        }
        this.sampleIndexSpecSet = Collections.unmodifiableSet(tempSampleIndexSpecSet);

        // determine if any of the sample index specs have an index 2
        this.sampleSpecSetHasIndex2 = this.sampleIndexSpecSet.stream().anyMatch(SampleIndexSpec::hasIndex2);

        // get the workdir path to work from
        this.workdirPath = Paths.get(inputObject.get(Input.workdirPathKey).toString()).toAbsolutePath();

        // get the bcl dir path
        this.bclPath = Paths.get(inputObject.get(Input.bclPathKey).toString()).toAbsolutePath();

        // get the processing Thread Number
        this.processingThreadNumber = Integer.parseInt(inputObject.get(Input.processingThreadNumberKey).toString());
    }
}
