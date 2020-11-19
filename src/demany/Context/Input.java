package demany.Context;

import demany.Program.Program;
import demany.SampleIndex.SampleIndexSpec;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class Input {

    // key values
    static final String programKey = "program";
    static final String sampleIndexSpecArrayKey = "sampleIndexSpecArray";
    static final String workdirPathKey = "workdirPath";
    static final String bclPathKey = "bclPath";
    static final String demultiplexingThreadNumberKey = "demultiplexingThreadNumber";

    // default values
    public static final int demultiplexingThreadNumberDefault = -1;

    // Note: a demultiplexingThreadNumberDefault of -1 means that there will be 1 demultiplexing thread per lane

    public final Program program;
    public final Set<SampleIndexSpec> sampleIndexSpecSet;
    public final boolean sampleSpecSetHasIndex2;
    public final Path workdirPath;
    public final Path bclPath;
    public final int demultiplexingThreadNumber;

    public Input(String jsonInput) throws Exception {

        // parse the input JSON
        JSONObject inputObject = (JSONObject) JSONValue.parse(jsonInput);

        // get the program to be run
        this.program = Program.valueOf(inputObject.get(Input.programKey).toString());

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

        // if we are demultiplexing a bcl dir, get its path and the workdir path
        if (this.program == Program.DEMULTIPLEX) {

            // get the workdir path to work from
            this.workdirPath = Paths.get(inputObject.get(Input.workdirPathKey).toString()).toAbsolutePath();
            this.bclPath = Paths.get(inputObject.get(Input.bclPathKey).toString()).toAbsolutePath();

            // get the demultiplexing Thread Number
            if (inputObject.containsKey(Input.demultiplexingThreadNumberKey)) {
                this.demultiplexingThreadNumber = Integer.parseInt(
                        inputObject.get(Input.demultiplexingThreadNumberKey).toString()
                );
            } else {
                this.demultiplexingThreadNumber = Input.demultiplexingThreadNumberDefault;
            }

        } else {

            this.workdirPath = null;
            this.bclPath = null;
            this.demultiplexingThreadNumber = 0;
        }
    }
}
