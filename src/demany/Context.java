package demany;

import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;
import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.*;
import java.util.stream.Collectors;

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
    public final Set<String> readTypeSet;
    public final Set<String> nonIndexReadTypeStrSet;
    public final Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr;
    public final Map<String, Set<SampleIdData>> sampleIdDataSetByLaneStr;
    public final Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr;
    public final Map<String, SampleIndexLookup> sampleIndexLookupByLaneStr;
    public final Map<String, Map<String, Map<String, Fastq>>> outputFastqByReadTypeByIdByLaneStr;

    public Context(
            Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
            Set<SampleIndexSpec> sampleIndexSpecSet,
            String index1ReadTypeStr,
            String index2ReadTypeStr,
            int index1Length,
            int index2Length,
            boolean index2ReverseCompliment,
            Path outputDirPath,
            int mutiplexedSequenceGroupSize
    ) throws IOException {

        // check input
        if (masterFastqByReadTypeByLaneStr.isEmpty()) {
            throw new RuntimeException("the master fastq map cannot be empty");
        }
        if (sampleIndexSpecSet.isEmpty()) {
            throw new RuntimeException("cannot have an empty sample index spec set");
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

        // get lane str by lane int map
        Map<Integer, String> laneStrByLaneInt = getLaneStrByLaneInt(
                new HashSet<>(masterFastqByReadTypeByLaneStr.keySet())
        );

        // set simple variables
        this.hasIndex2 = index2Length == 0;
        this.index1ReadTypeStr = index1ReadTypeStr;
        this.index2ReadTypeStr = index2ReadTypeStr;
        this.index1Length = index1Length;
        this.index2Length = index2Length;
        this.index2ReverseCompliment = index2ReverseCompliment;
        this.outputDirPath = outputDirPath;
        this.mutiplexedSequenceGroupSize = mutiplexedSequenceGroupSize;
        this.demultiplexedSequenceGroupSize =
                laneStrByLaneInt.size() * mutiplexedSequenceGroupSize / sampleIndexSpecSet.size();

        // get the read type set from the master fastq map
        this.readTypeSet = masterFastqByReadTypeByLaneStr.values().stream()
                .flatMap(v -> v.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());

        // vallidate and create an unmodifiable view of the master fastq map
        this.masterFastqByReadTypeByLaneStr = getUnmodifiableMasterFastqByReadTypeByLaneStr(
                masterFastqByReadTypeByLaneStr
        );

        // create the non-index read type str set
        HashSet<String> modifiableNonIndexReadTypeStrSet = new HashSet<>();
        for (String readTypeString : this.readTypeSet) {
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

        // set unmodifiable views of the sample maps on this context intance
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

        // set an unmodifiable view of the sample index lookup by lane map on this context instance
        this.sampleIndexLookupByLaneStr = Collections.unmodifiableMap(modifiableSampleIndexLookupByLaneStr);

        // create the map of output fastqs
        Map<String, Map<String, Map<String, Fastq>>> modifiableOutputFastqByReadTypeByIdByLaneStr = new HashMap<>();
        for (String laneStr : laneStrByLaneInt.values()) {

            Map<String, Map<String, Fastq>> outputFastqByReadTypeById = new HashMap<>();

            // add undetermined fastqs for this lane
            outputFastqByReadTypeById.put(
                    Context.undeterminedId,
                    getUndeterminedOutputFastqByReadTypeForLane(laneStr)
            );

            // add sample fastqs for each sample in this lane
            for (SampleIdData sampleIdData : this.sampleIdDataSetByLaneStr.get(laneStr)) {

                outputFastqByReadTypeById.put(
                        sampleIdData.id,
                        getSampleOutputFastqByReadTypeForLane(sampleIdData, laneStr)
                );
            }

            // add the map for this lane to the parent map
            modifiableOutputFastqByReadTypeByIdByLaneStr.put(
                    laneStr,
                    Collections.unmodifiableMap(outputFastqByReadTypeById)
            );
        }

        // set an unmodifiable view of the output fastq map on this context intance
        this.outputFastqByReadTypeByIdByLaneStr = Collections.unmodifiableMap(
                modifiableOutputFastqByReadTypeByIdByLaneStr
        );
    }

    private static Map<Integer, String> getLaneStrByLaneInt(Set<String> laneStrSet) {

        Map<Integer, String> resultMap = new HashMap<>();
        for (String laneStr : laneStrSet) {

            int laneInt = Fastq.getLaneIntFromLaneStr(laneStr);
            resultMap.put(laneInt, laneStr);
        }

        return resultMap;
    }

    private Map<String, Map<String, Fastq>> getUnmodifiableMasterFastqByReadTypeByLaneStr(
            Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr) {

        // first check to make sure that each lane has the known read types
        for (String laneStr : masterFastqByReadTypeByLaneStr.keySet()) {

            Set<String> thisReadTypeSet = new HashSet<>(masterFastqByReadTypeByLaneStr.get(laneStr).keySet());

            if (!this.readTypeSet.equals(thisReadTypeSet)) {
                throw new RuntimeException("not all the lanes have the same read types");
            }
        }

        // return an unmodifiable view
        masterFastqByReadTypeByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableMap(masterFastqByReadTypeByLaneStr.get(k))
        );

        return Collections.unmodifiableMap(masterFastqByReadTypeByLaneStr);
    }

    private Map<String, Fastq> getUndeterminedOutputFastqByReadTypeForLane(String laneStr) {

        HashMap<String, Fastq> resultMap = new HashMap<>();

        for (String readTypeStr : this.readTypeSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getUndeterminedFastqAtDir(this.outputDirPath, laneStr, readTypeStr)
            );
        }

        return Collections.unmodifiableMap(resultMap);
    }

    private Map<String, Fastq> getSampleOutputFastqByReadTypeForLane(SampleIdData sampleIdData, String laneStr)
            throws IOException {

        // create the dir for this sample
        Path sampleOutputDir = this.outputDirPath.resolve(sampleIdData.project).resolve(sampleIdData.sample);
        Files.createDirectories(sampleOutputDir);

        // create the fastq by id map
        HashMap<String, Fastq> resultMap = new HashMap<>();

        // add a fastq to the map for each read type
        for (String readTypeStr : this.readTypeSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getSampleFastqAtDir(sampleOutputDir, sampleIdData.sample, laneStr, readTypeStr)
            );
        }

        return Collections.unmodifiableMap(resultMap);
    }
}
