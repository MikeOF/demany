package demany;

import demany.SampleIndex.SampleIndexSpec;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;

public class Input {

    public enum Program { CHECK_INDICES, DEMULTIPLEX }

    private final Input.Program program;
    private final HashSet<SampleIndexSpec> sampleIndexSpecSet;
    private final Path workdirPath;
    private final Path bclPath;

    public Input(String jsonInput) throws Exception {

        // parse the input JSON
        JSONObject inputObject = (JSONObject) JSONValue.parse(jsonInput);

        // get the program to be run
        program = Input.Program.valueOf(inputObject.get("program").toString());

        // create the sample index spec array
        JSONArray sampleIndexSpecJSONArray = (JSONArray) inputObject.get("sampleIndexSpecArray");

        // collect sample index specs
        sampleIndexSpecSet = new HashSet<>();
        for (Object sampleIndexSpecObject : sampleIndexSpecJSONArray) {

            sampleIndexSpecSet.add(new SampleIndexSpec((JSONObject) sampleIndexSpecObject));
        }

        // get the workdir path to work from
        workdirPath = Paths.get(inputObject.get("workdirPath").toString()).toAbsolutePath();

        // if we are demultiplexing a bcl dir, get its path
        if (program == Input.Program.DEMULTIPLEX) {
            bclPath = Paths.get(inputObject.get("bclPath").toString()).toAbsolutePath();
        } else {
            bclPath = null;
        }
    }

    public String getProgram() {
        return program.toString();
    }

    public HashSet<SampleIndexSpec> getSampleIndexSpecs() {
        return new HashSet<>(sampleIndexSpecSet);
    }

    public Path getWorkdirPath() { return workdirPath; }

    public Path getBclPath() { return bclPath; }

}
