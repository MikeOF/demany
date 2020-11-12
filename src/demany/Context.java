package demany;

import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;
import demany.Utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;

public class Context {

    public static class SampleIdData {

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

    public static final String undeterminedId = "undetermined";
    public final boolean hasIndex2;
    public final Path outputDirPath;
    public final String index1ReadTypeStr;
    public final String index2ReadTypeStr;
    public final int index1Length;
    public final int index2Length;
    public final boolean index2ReverseCompliment;
    public final int mutiplexedSequenceGroupSize;
    public final int demultiplexedSequenceGroupSize;
    public final Set<String> readTypeStrSet;
    public final Set<String> nonIndexReadTypeStrSet;
    public final Map<String, Set<SampleIdData>> sampleIdDataSetByLaneStr;
    public final Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr;
    public final Map<String, SampleIndexLookup> sampleIndexLookupByLaneStr;

    public Context(
            Set<SampleIndexSpec> sampleIndexSpecSet,
            Map<Integer, String> laneStrByLaneInt,
            Set<String> readTypeSet,
            String index1ReadTypeStr,
            String index2ReadTypeStr,
            int index1Length,
            int index2Length,
            boolean index2ReverseCompliment,
            Path outputDirPath,
            int mutiplexedSequenceGroupSize
    ) throws IOException {

        // check input
        if (sampleIndexSpecSet.isEmpty()) {
            throw new RuntimeException("cannot have an empty sample index spec set");
        }
        if (laneStrByLaneInt.isEmpty()) {
            throw new RuntimeException("cannot have an empty lane str by lane int set");
        }
        if (readTypeSet.isEmpty()) {
            throw new RuntimeException("cannot have an empyt read type set");
        }
        if (index1Length < 1) {
            throw new RuntimeException("index 1 length cannot be less than 1");
        }
        if (index2Length < 0) {
            throw new RuntimeException("index 2 length cannot be less than 0");
        }
        if (!Files.isDirectory(outputDirPath) || !outputDirPath.isAbsolute()) {
            throw new RuntimeException("output dir path must be an absolute path to an existant directory");
        }
        if (index2ReverseCompliment && index2Length == 0) {
            throw new RuntimeException("if index 2 is reverse compliment it cannot be of length 0");
        }
        if ((index2ReadTypeStr == null && index2Length != 0) || (index2ReadTypeStr != null && index2Length < 1)) {
            throw new RuntimeException(
                    "index 2 read type must be null if index 2 length is 0 and non-null if index 2 length < 1"
            );
        }
        if (mutiplexedSequenceGroupSize < 0) {
            throw new RemoteException("mutiplexedSequenceGroupSize must be greater than -1");
        }

        // set simple variables
        this.hasIndex2 = index2Length == 0;
        this.index1ReadTypeStr = index1ReadTypeStr;
        this.index2ReadTypeStr = index2ReadTypeStr;
        this.index1Length = index1Length;
        this.index2Length = index2Length;
        this.index2ReverseCompliment = index2ReverseCompliment;
        this.outputDirPath = outputDirPath;
        this.readTypeStrSet = Collections.unmodifiableSet(readTypeSet);
        this.mutiplexedSequenceGroupSize = mutiplexedSequenceGroupSize;
        this.demultiplexedSequenceGroupSize = laneStrByLaneInt.size() * mutiplexedSequenceGroupSize / sampleIndexSpecSet.size();

        // create the non-index read type str set
        HashSet<String> modifiableNonIndexReadTypeStrSet = new HashSet<>();
        for (String readTypeString : this.readTypeStrSet) {
            if (!readTypeString.equals(this.index1ReadTypeStr) && !readTypeString.equals(this.index2ReadTypeStr)) {
                modifiableNonIndexReadTypeStrSet.add(readTypeString);
            }
        }
        this.nonIndexReadTypeStrSet = Collections.unmodifiableSet(modifiableNonIndexReadTypeStrSet);

        // create the sample id data and sample index spec maps
        Map<String, Set<SampleIdData>> modifiableSampleIdDataSetByLaneStr = new HashMap<>();
        Map<String, Set<SampleIndexSpec>> modifiableSampleIndexSpecSetByLaneStr = new HashMap<>();
        for (SampleIndexSpec sampleIndexSpec : sampleIndexSpecSet) {

            // create the sample data
            SampleIdData sampleIdData = new SampleIdData(
                    Utils.getIdForProjectSample(sampleIndexSpec.project, sampleIndexSpec.sample),
                    sampleIndexSpec.project,
                    sampleIndexSpec.sample
            );

            // get the lane string for this sample index's lane int
            String laneStr = laneStrByLaneInt.get(sampleIndexSpec.lane);

            // add sets to maps if necessary
            if (!modifiableSampleIdDataSetByLaneStr.containsKey(laneStr)) {
                modifiableSampleIdDataSetByLaneStr.put(laneStr, new HashSet<>());
                modifiableSampleIndexSpecSetByLaneStr.put(laneStr, new HashSet<>());
            }

            // add sample id data and index spec to maps
            modifiableSampleIdDataSetByLaneStr.get(laneStr).add(sampleIdData); // duplicate adds are okay
            modifiableSampleIndexSpecSetByLaneStr.get(laneStr).add(sampleIndexSpec);
        }

        // set unmodifiable views of the sample maps on this context object
        modifiableSampleIdDataSetByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableSet(modifiableSampleIdDataSetByLaneStr.get(k))
        );
        modifiableSampleIndexSpecSetByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableSet(modifiableSampleIndexSpecSetByLaneStr.get(k))
        );
        this.sampleIdDataSetByLaneStr = Collections.unmodifiableMap(modifiableSampleIdDataSetByLaneStr);
        this.sampleIndexSpecSetByLaneStr = Collections.unmodifiableMap(modifiableSampleIndexSpecSetByLaneStr);

        // create the lookup by lane str map
        Map<String, SampleIndexLookup> modifiableSampleIndexLookupByLaneStr = new HashMap<>();
        for (String laneStr : laneStrByLaneInt.values()) {

            modifiableSampleIndexLookupByLaneStr.put(
                    laneStr,
                    new SampleIndexLookup(
                            new SampleIndexKeyMappingCollection(
                                    this.sampleIndexSpecSetByLaneStr.get(laneStr), index1Length, index2Length
                            ),
                            this.index2ReverseCompliment
                    )
            );
        }

        // set an unmodifiable view of the sample index lookup by lane map on this context object
        this.sampleIndexLookupByLaneStr = Collections.unmodifiableMap(modifiableSampleIndexLookupByLaneStr);
    }
}
