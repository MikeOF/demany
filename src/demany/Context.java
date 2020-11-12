package demany;

import demany.DataFlow.FastqWriterGroup;
import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;
import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Context {

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

    public static final String undeterminedId = "undetermined";
    public final boolean hasIndex2;
    public final Path outputDirPath;
    public final int index1Length;
    public final int index2Length;
    public final boolean index2ReverseCompliment;
    public final Set<String> readTypeStrSet;
    public final Map<String, Set<SampleIdData>> sampleIdDataSetByLaneStr;
    public final Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr;
    public final Map<String, Map<String, FastqWriterGroup>> fastqWriterGroupByIdByLaneStr;
    public final Map<String, SampleIndexLookup> sampleIndexLookupByLaneStr;


    public Context(
            Set<SampleIndexSpec> sampleIndexSpecSet, Map<Integer, String> laneStrByLaneInt,
            Set<String> readTypeSet, int index1Length, int index2Length, boolean index2ReverseCompliment,
            Path outputDirPath
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

        // set simple variables
        this.hasIndex2 = index2Length == 0;
        this.index1Length = index1Length;
        this.index2Length = index2Length;
        this.index2ReverseCompliment = index2ReverseCompliment;
        this.outputDirPath = outputDirPath;
        this.readTypeStrSet = Collections.unmodifiableSet(readTypeSet);

        // create the sample data maps
        Map<String, Set<SampleIdData>> modifiableSampleIdDataSetByLaneStr = new HashMap<>();
        Map<String, Set<SampleIndexSpec>> modifiableSampleIndexSpecSetByLaneStr = new HashMap<>();
        for (SampleIndexSpec sampleIndexSpec : sampleIndexSpecSet) {

            // create the sample data
            String project = sampleIndexSpec.project;
            String sample = sampleIndexSpec.sample;
            String id = Utils.getIdForProjectSample(project, sample);

            SampleIdData sampleIdData = new SampleIdData(id, project, sample);

            // add to the map
            String laneStr = laneStrByLaneInt.get(sampleIndexSpec.lane);

            if (!modifiableSampleIdDataSetByLaneStr.containsKey(laneStr)) {
                modifiableSampleIdDataSetByLaneStr.put(laneStr, new HashSet<>());
                modifiableSampleIndexSpecSetByLaneStr.put(laneStr, new HashSet<>());
            }

            modifiableSampleIdDataSetByLaneStr.get(laneStr).add(sampleIdData); // duplicate adds are okay
            modifiableSampleIndexSpecSetByLaneStr.get(laneStr).add(sampleIndexSpec);
        }

        // set unmodifiable views of the sample data maps on this context object
        modifiableSampleIdDataSetByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableSet(modifiableSampleIdDataSetByLaneStr.get(k))
        );
        modifiableSampleIndexSpecSetByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableSet(modifiableSampleIndexSpecSetByLaneStr.get(k))
        );
        this.sampleIdDataSetByLaneStr = Collections.unmodifiableMap(modifiableSampleIdDataSetByLaneStr);
        this.sampleIndexSpecSetByLaneStr = Collections.unmodifiableMap(modifiableSampleIndexSpecSetByLaneStr);

        // create the fastq writer by id by lane str map
        Map<String, Map<String, FastqWriterGroup>> modifiableFastqWriterGroupByIdByLaneStr = new HashMap<>();
        for (String laneStr : laneStrByLaneInt.values()) {

            HashMap<String, FastqWriterGroup> fastqWriterGroupById = new HashMap<>();

            // add an undetermined writer group for this lane
            fastqWriterGroupById.put(
                    Context.undeterminedId,
                    new FastqWriterGroup(getUndeterminedOutputFastqsForLane(laneStr))
            );

            // add a fastq writer group for each sample in this lane
            for (SampleIdData sampleIdData : this.sampleIdDataSetByLaneStr.get(laneStr)) {

                fastqWriterGroupById.put(
                        sampleIdData.id,
                        new FastqWriterGroup(getSampleOutputFastqsForLane(sampleIdData, laneStr))
                );
            }

            modifiableFastqWriterGroupByIdByLaneStr.put(laneStr, fastqWriterGroupById);
        }

        // set unmodifiable view of fastq writer by id by lant str on this context object
        modifiableFastqWriterGroupByIdByLaneStr.replaceAll(
                (k, v) -> Collections.unmodifiableMap(modifiableFastqWriterGroupByIdByLaneStr.get(k))
        );
        this.fastqWriterGroupByIdByLaneStr = Collections.unmodifiableMap(modifiableFastqWriterGroupByIdByLaneStr);

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

        // set an unmodifiable view of the sample index lookup my lane map on this context object
        this.sampleIndexLookupByLaneStr = Collections.unmodifiableMap(modifiableSampleIndexLookupByLaneStr);
    }

    private HashMap<String, Fastq> getUndeterminedOutputFastqsForLane(String laneStr) {

        HashMap<String, Fastq> resultMap = new HashMap<>();

        for (String readTypeStr : this.readTypeStrSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getUndeterminedFastqAtDir(this.outputDirPath, laneStr, readTypeStr)
            );
        }

        return resultMap;
    }

    private HashMap<String, Fastq> getSampleOutputFastqsForLane(SampleIdData sampleIdData, String laneStr) throws IOException {

        // create the dir for this sample
        Path sampleOutputDir = this.outputDirPath.resolve(sampleIdData.project).resolve(sampleIdData.sample);
        Files.createDirectories(sampleOutputDir);

        // create the fastq by id map
        HashMap<String, Fastq> resultMap = new HashMap<>();

        // add a fastq to the map for each read type
        for (String readTypeStr : this.readTypeStrSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getSampleFastqAtDir(sampleOutputDir, sampleIdData.sample, laneStr, readTypeStr)
            );
        }

        return resultMap;
    }
}
