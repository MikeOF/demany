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

    // key values
    static final String programKey = "program";
    static final String sampleIndexSpecArrayKey = "sampleIndexSpecArray";
    static final String workdirPathKey = "workdirPath";
    static final String bclPathKey = "bclPath";
    static final String demultiplexingThreadNumberKey = "demultiplexingThreadNumber";
    static final String sequenceChunkSizeKey = "sequenceChunkSize";
    static final String sequenceChunkQueueSizeKey = "sequenceChunkQueueSize";

    // default values
    public static final int demultiplexingThreadNumberDefault = -1;
    public static final int sequenceChunkSizeDefault = 20000;
    public static final int sequenceChunkQueueSizeDefault = 5;

    // Note: a demultiplexingThreadNumberDefault of -1 means that there will be 1 demultiplexing thread per lane

    public enum Program { CHECK_INDICES, DEMULTIPLEX }

    public final Input.Program program;
    public final Set<SampleIndexSpec> sampleIndexSpecSet;
    public final Path workdirPath;
    public final Path bclPath;
    public final int demultiplexingThreadNumber;
    public final int sequenceChunkSize;
    public final int sequenceChunkQueueSize;

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

            // get the demultiplexing Thread Number
            if (inputObject.containsKey(Input.demultiplexingThreadNumberKey)) {
                this.demultiplexingThreadNumber = Integer.parseInt(
                        inputObject.get(Input.demultiplexingThreadNumberKey).toString()
                );
            } else {
                this.demultiplexingThreadNumber = Input.demultiplexingThreadNumberDefault;
            }

            // get the sequence chunk size
            if (inputObject.containsKey(Input.sequenceChunkSizeKey)) {
                this.sequenceChunkSize = Integer.parseInt(inputObject.get(Input.sequenceChunkSizeKey).toString());
            } else {
                this.sequenceChunkSize = Input.sequenceChunkSizeDefault;
            }

            // get the sequence chunk queue size
            if (inputObject.containsKey(Input.sequenceChunkQueueSizeKey)) {
                this.sequenceChunkQueueSize = Integer.parseInt(
                        inputObject.get(Input.sequenceChunkQueueSizeKey).toString()
                );
            } else {
                this.sequenceChunkQueueSize = Input.sequenceChunkQueueSizeDefault;
            }

        } else {

            this.workdirPath = null;
            this.bclPath = null;
            this.demultiplexingThreadNumber = 0;
            this.sequenceChunkSize = 0;
            this.sequenceChunkQueueSize = 0;
        }
    }
}
