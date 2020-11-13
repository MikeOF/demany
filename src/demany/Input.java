package demany;

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

    static final String programKey = "program";
    static final String sampleIndexSpecArrayKey = "sampleIndexSpecArray";
    static final String workdirPathKey = "workdirPath";
    static final String bclPathKey = "bclPath";

    public enum Program { CHECK_INDICES, DEMULTIPLEX }

    public final Input.Program program;
    public final Set<SampleIndexSpec> sampleIndexSpecSet;
    public final Path workdirPath;
    public final Path bclPath;

    public Input(String jsonInput) throws Exception {

        // parse the input JSON
        JSONObject inputObject = (JSONObject) JSONValue.parse(jsonInput);

        // get the program to be run
        this.program = Input.Program.valueOf(inputObject.get(Input.programKey).toString());

        // create the sample index spec array
        JSONArray sampleIndexSpecJSONArray = (JSONArray) inputObject.get(Input.sampleIndexSpecArrayKey);

        // collect sample index specs
        Set<SampleIndexSpec> tempSampleIndexSpecSet = new HashSet<>();
        for (Object sampleIndexSpecObject : sampleIndexSpecJSONArray) {

            tempSampleIndexSpecSet.add(new SampleIndexSpec((JSONObject) sampleIndexSpecObject));
        }
        this.sampleIndexSpecSet = Collections.unmodifiableSet(tempSampleIndexSpecSet);

        // if we are demultiplexing a bcl dir, get its path and the workdir path
        if (this.program == Input.Program.DEMULTIPLEX) {

            // get the workdir path to work from
            this.workdirPath = Paths.get(inputObject.get(Input.workdirPathKey).toString()).toAbsolutePath();
            this.bclPath = Paths.get(inputObject.get(Input.bclPathKey).toString()).toAbsolutePath();

        } else {

            this.workdirPath = null;
            this.bclPath = null;
        }
    }
}
