package demany.Context;

import demany.Fastq.Fastq;
import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class DemultiplexingContext {

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

    public static final String UNDETERMINED_ID = "undetermined";

    public final boolean hasIndex2;
    public final Path demultiplexedFastqsDirPath;
    public final Path indexCountsDirPath;
    public final String index1ReadType;
    public final String index2ReadType;
    public final int index1Length;
    public final int index2Length;
    public final boolean index2ReverseCompliment;
    public final Set<String> readTypeSet;
    public final Set<String> nonIndexReadTypeSet;
    public final Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr;
    public final Map<String, Set<SampleIdData>> sampleIdDataSetByLaneStr;
    public final Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr;
    public final Map<String, SampleIndexLookup> sampleIndexLookupByLaneStr;
    public final Map<String, Map<String, Map<String, Fastq>>> outputFastqByReadTypeByIdByLaneStr;

    public DemultiplexingContext(
            Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
            Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr,
            int index1Length,
            int index2Length,
            boolean index2ReverseCompliment,
            Path demultiplexedFastqsDirPath,
            Path indexCountsDirPath
    ) throws IOException {

        // check input
        if (masterFastqByReadTypeByLaneStr.isEmpty()) {
            throw new RuntimeException("the master fastq map cannot be empty");
        }
        if (sampleIndexSpecSetByLaneStr.isEmpty()) {
            throw new RuntimeException("cannot have an empty sample index spec by lane str map");
        }
        if (index1Length < 1) {
            throw new RuntimeException("index 1 length cannot be less than 1");
        }
        if (index2Length < 0) {
            throw new RuntimeException("index 2 length cannot be less than 0");
        }
        if (!Files.isDirectory(demultiplexedFastqsDirPath) || !demultiplexedFastqsDirPath.isAbsolute()) {
            throw new RuntimeException("output dir path must be an absolute path to an existant directory");
        }
        if (!Files.isDirectory(indexCountsDirPath) || !indexCountsDirPath.isAbsolute()) {
            throw new RuntimeException("index counts dir path must be an absolute path to an existant directory");
        }
        if (index2ReverseCompliment && index2Length == 0) {
            throw new RuntimeException("if index 2 is reverse compliment it cannot be of length 0");
        }

        this.masterFastqByReadTypeByLaneStr = masterFastqByReadTypeByLaneStr;
        this.sampleIndexSpecSetByLaneStr = sampleIndexSpecSetByLaneStr;
        this.hasIndex2 = index2Length != 0;
        this.index1Length = index1Length;
        this.index2Length = index2Length;
        this.index2ReverseCompliment = index2ReverseCompliment;
        this.demultiplexedFastqsDirPath = demultiplexedFastqsDirPath;
        this.indexCountsDirPath = indexCountsDirPath;

        // get an unmodifiable view of a read type set from the master fastq map
        this.readTypeSet = this.masterFastqByReadTypeByLaneStr.values().stream()
                .flatMap(v -> v.keySet().stream())
                .collect(Collectors.toUnmodifiableSet());

        // determine index read types
        String[] indexReadTypes = getIndexReadTypes();
        this.index1ReadType = indexReadTypes[0];
        if (hasIndex2) { this.index2ReadType = indexReadTypes[1]; }
        else { this.index2ReadType = null; }

        // set unmodifiable view of a non-index read type map on this context instance
        this.nonIndexReadTypeSet = this.readTypeSet.stream()
                .filter(v -> !this.index1ReadType.equals(v))
                .filter(v -> this.index2ReadType == null || !this.index2ReadType.equals(v))
                .collect(Collectors.toUnmodifiableSet());

        // set unmodifiable views of sample id data and sample index spec maps on this context instance
        this.sampleIdDataSetByLaneStr = getSampleIdDataSetByLaneStr();

        // set an unmodifiable view of an sample index lookup by lane map on this context instance
        this.sampleIndexLookupByLaneStr = getSampleIndexLookupByLaneStr();

        // set an unmodifiable view of an output fastq map on this context intance
        this.outputFastqByReadTypeByIdByLaneStr = getOutputFastqByReadTypeByIdByLaneStr();
    }

    private String[] getIndexReadTypes() {

        if (this.hasIndex2) {

            // make sure the read type set containes both the index 1 and 2 type strings that we expect
            if (!this.readTypeSet.contains(Fastq.INDEX_1_READ_TYPE_STR) ||
                    !this.readTypeSet.contains(Fastq.INDEX_2_READ_TYPE_STR)) {

                throw new RuntimeException(
                        "dual index read type set does not contain the expected index 1 and index 2 read types, " +
                                readTypeSet.toString()
                );
            }

            return new String[]{Fastq.INDEX_1_READ_TYPE_STR, Fastq.INDEX_2_READ_TYPE_STR};

        } else {

            // make sure the read type set has the expected index 1 type string
            if (!this.readTypeSet.contains(Fastq.INDEX_1_READ_TYPE_STR)) {

                throw new RuntimeException(
                        "single index read type set does not contain the expected index 1 read type, " +
                                readTypeSet.toString()
                );
            }

            // make sure the read type set does not contain the index 2 read type string
            if (this.readTypeSet.contains(Fastq.INDEX_2_READ_TYPE_STR)) {

                throw new RuntimeException(
                        "single index read type set contains the index 2 read type, " + readTypeSet.toString()
                );
            }

            return new String[]{Fastq.INDEX_1_READ_TYPE_STR};
        }
    }

    private Map<String, Set<SampleIdData>> getSampleIdDataSetByLaneStr() {

        // create the sample id data map
        Map<String, Set<SampleIdData>> modifiableSampleIdDataSetByLaneStr = new HashMap<>();
        for (String laneStr : this.sampleIndexSpecSetByLaneStr.keySet()) {

            // add lane str to the map
            modifiableSampleIdDataSetByLaneStr.put(laneStr, new HashSet<>());

            // create and add a sample id data to the new set for each sample index spec in the lane
            for (SampleIndexSpec sampleIndexSpec : this.sampleIndexSpecSetByLaneStr.get(laneStr)) {

                modifiableSampleIdDataSetByLaneStr.get(laneStr).add(
                        new SampleIdData(sampleIndexSpec.id, sampleIndexSpec.project, sampleIndexSpec.sample)
                );
            }
        }

        // set unmodifiable views of the sample maps on this context intance
        modifiableSampleIdDataSetByLaneStr.replaceAll((k,v)->Collections.unmodifiableSet(v));

        return Collections.unmodifiableMap(modifiableSampleIdDataSetByLaneStr);
    }

    private Map<String, SampleIndexLookup> getSampleIndexLookupByLaneStr() {

        Map<String, SampleIndexLookup> sampleIndexLookupByLaneStr = new HashMap<>();
        for (String laneStr : this.sampleIndexSpecSetByLaneStr.keySet()) {

            sampleIndexLookupByLaneStr.put(
                    laneStr,
                    new SampleIndexLookup(
                            new SampleIndexKeyMappingCollection(
                                    this.sampleIndexSpecSetByLaneStr.get(laneStr), index1Length, index2Length
                            ),
                            this.index2ReverseCompliment
                    )
            );
        }

        return Collections.unmodifiableMap(sampleIndexLookupByLaneStr);
    }

    private Map<String, Map<String, Map<String, Fastq>>> getOutputFastqByReadTypeByIdByLaneStr() throws IOException {

        // create the map
        Map<String, Map<String, Map<String, Fastq>>> outputFastqByReadTypeByIdByLaneStr = new HashMap<>();
        for (String laneStr : this.masterFastqByReadTypeByLaneStr.keySet()) {

            Map<String, Map<String, Fastq>> outputFastqByReadTypeById = new HashMap<>();

            // add undetermined fastqs for this lane
            outputFastqByReadTypeById.put(
                    DemultiplexingContext.UNDETERMINED_ID,
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
            outputFastqByReadTypeByIdByLaneStr.put(
                    laneStr,
                    Collections.unmodifiableMap(outputFastqByReadTypeById)
            );
        }

        // return an unmodifiable view
        return Collections.unmodifiableMap(outputFastqByReadTypeByIdByLaneStr);
    }

    private Map<String, Fastq> getUndeterminedOutputFastqByReadTypeForLane(String laneStr) {

        HashMap<String, Fastq> resultMap = new HashMap<>();

        for (String readTypeStr : this.readTypeSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getUndeterminedFastqAtDir(this.demultiplexedFastqsDirPath, laneStr, readTypeStr)
            );
        }

        return Collections.unmodifiableMap(resultMap);
    }

    private Map<String, Fastq> getSampleOutputFastqByReadTypeForLane(SampleIdData sampleIdData, String laneStr)
            throws IOException {

        // create the dir for this sample
        Path sampleOutputDir =
                this.demultiplexedFastqsDirPath.resolve(sampleIdData.project).resolve(sampleIdData.sample);
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
