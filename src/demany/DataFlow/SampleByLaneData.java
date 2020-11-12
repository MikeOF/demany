package demany.DataFlow;

import demany.SampleIndex.SampleIndexSpec;
import demany.Utils.Utils;

import java.util.*;

public class SampleByLaneData {

    static class SampleIdData {

            public final String id;
            public final String project;
            public final String sample;

            SampleIdData(String id, String project, String sample) {
                this.id = id;
                this.project = project;
                this.sample = sample;
            }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;

            SampleIdData other = (SampleIdData) object;

            return id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    private final HashMap<String, HashSet<SampleIdData>> sampleDataSetByLaneStr = new HashMap<>();
    private final HashMap<String, HashSet<SampleIndexSpec>> sampleIndexSpecByLaneStr = new HashMap<>();

    public SampleByLaneData(HashSet<SampleIndexSpec> sampleIndexSpecSet, HashMap<Integer, String> laneStrByLaneInt) {

        // create the maps
        for (SampleIndexSpec sampleIndexSpec : sampleIndexSpecSet) {

            // create the sample data
            String project = sampleIndexSpec.project;
            String sample = sampleIndexSpec.sample;
            String id = Utils.getIdForProjectSample(project, sample);

            SampleIdData sampleIdData = new SampleIdData(id, project, sample);

            // add to the map
            String laneStr = laneStrByLaneInt.get(sampleIndexSpec.lane);

            if (!this.sampleDataSetByLaneStr.containsKey(laneStr)) {
                this.sampleDataSetByLaneStr.put(laneStr, new HashSet<>());
                this.sampleIndexSpecByLaneStr.put(laneStr, new HashSet<>());
            }

            this.sampleDataSetByLaneStr.get(laneStr).add(sampleIdData); // duplicate adds are okay
            this.sampleIndexSpecByLaneStr.get(laneStr).add(sampleIndexSpec);
        }
    }

    public HashSet<SampleIdData> getSampleDataSetForLaneStr(String laneStr) {
        return new HashSet<>(sampleDataSetByLaneStr.get(laneStr));
    }

    public HashSet<SampleIndexSpec> getSampleIndexSpecSetForLaneStr(String laneStr) {
        return new HashSet<>(sampleIndexSpecByLaneStr.get(laneStr));
    }
}
