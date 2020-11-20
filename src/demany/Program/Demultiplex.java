package demany.Program;

import demany.Context.BCLParameters;
import demany.Context.DemultiplexingContext;
import demany.Context.Input;
import demany.Fastq.Fastq;
import demany.Fastq.SequenceGroupFlow;
import demany.Fastq.SequenceLines;
import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;
import demany.Threading.DemultiplexingThread;
import demany.Threading.ReaderThread;
import demany.Threading.WriterThread;
import demany.Utils.Utils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Demultiplex {

    private static final Logger LOGGER = Logger.getLogger( Demultiplex.class.getName() );

    public static int ExecuteDemultiplex(Input input)
            throws IOException, SAXException, ParserConfigurationException, InterruptedException {

        // print time
        LOGGER.log(Level.INFO,"\n\n ---- Start of Demultiplexing Process ----\n");

        // check input
        if (input.sampleIndexSpecSet.isEmpty()) {
            throw new RuntimeException("sample index spec set is empty");
        }
        if (input.processingThreadNumber < 1) {
            throw new RuntimeException("the processing thread number must be greater than 1");
        }
        if (Files.exists(input.workdirPath)) {
            throw new RuntimeException(
                    "workdir path already exists, stopping to avoid overwrite, " + input.workdirPath.toString()
            );
        }
        if (!Files.isDirectory(input.bclPath)) {
            throw new RuntimeException("bcl path is not to an existant directory, " + input.bclPath.toString());
        }
        if (!Files.exists(input.bclPath.resolve("RTAComplete.txt"))) {
            throw new RuntimeException("bcl path is not to a completed sequencing run, no RTAComplete.txt file found");
        }

        // create the workdir
        Files.createDirectory(input.workdirPath);

        // determine the run parameters
        BCLParameters bclParameters = new BCLParameters(input.bclPath);

        // run bcl2fastq
        Path bcl2fastqOutputDirPath = runBcl2fastq(bclParameters, input);

        // get master fastq by read type by lane str map
        Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr = getMasterFastqByReadTypeByLaneStr(
                bcl2fastqOutputDirPath
        );

        // get lane str by lane int map
        Map<Integer, String> laneStrByLaneInt = getLaneStrByLaneInt(masterFastqByReadTypeByLaneStr);

        // get sample index spec set by lane str map
        Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr = getSampleIndexSpecSetByLaneStr(
                input.sampleIndexSpecSet, laneStrByLaneInt
        );

        // determine the index 2 reverse compliment parameter
        boolean index2ReverseCompliment = determineIndex2ReverseCompliment(
                input, bclParameters, masterFastqByReadTypeByLaneStr, sampleIndexSpecSetByLaneStr
        );

        // determine the demultiplexing context
        DemultiplexingContext demultiplexingContext = determineDemultiplexingContext(
                input,
                bclParameters,
                masterFastqByReadTypeByLaneStr,
                sampleIndexSpecSetByLaneStr,
                index2ReverseCompliment
        );

        // demultiplex the master fastqs
        Map<String, Map<String, Map<String, Long>>> countByIndexStrByIdByLaneStr =
                demultiplexMasterFastqs(input, demultiplexingContext);

        // write out the count by index str by sample id by lane string results
        writeIndexCounts(demultiplexingContext, countByIndexStrByIdByLaneStr);

        // write out the total counts file
        writeTotalCounts(demultiplexingContext, countByIndexStrByIdByLaneStr);

        LOGGER.log(Level.INFO,"\n\n ---- End of Demultiplexing Process ----\n");

        return 0;
    }

    private static Path runBcl2fastq(BCLParameters bclParameters, Input input)
            throws InterruptedException, IOException {

        // create paths
        Path sampleSheetPath = input.workdirPath.resolve("bcl2fastq_sample_sheet.csv");
        Path outputDirPath = input.workdirPath.resolve("bcl2fastq-output");
        Path interopDirPath = input.workdirPath.resolve("bcl2fastq-interop");
        Path statsDirPath = input.workdirPath.resolve("bcl2fastq-stats");
        Path reportsDirPath = input.workdirPath.resolve("bcl2fastq-reports");

        // get all the lanes to demultiplex
        Set<Integer> laneIntSet = input.sampleIndexSpecSet.stream().map(v -> v.lane).collect(Collectors.toSet());

        // define the master sample sheet
        StringBuilder sampleSheetBuilder = new StringBuilder();

        sampleSheetBuilder.append("[Data]\n");

        if (bclParameters.hasIndex2 && input.sampleSpecSetHasIndex2) {

            sampleSheetBuilder.append("Sample_ID,lane,index,index2\n");

            for (int lane : laneIntSet) { sampleSheetBuilder.append("master,").append(lane).append(",,\n"); }

        } else {

            sampleSheetBuilder.append("Sample_ID,lane,index\n");

            for (int lane : laneIntSet) { sampleSheetBuilder.append("master,").append(lane).append(",\n"); }
        }

        String sampleSheet = sampleSheetBuilder.toString();

        // log and write the sample sheet
        LOGGER.log(Level.INFO, "\n\nSample Sheet:\n\n{0}", sampleSheet);

        BufferedWriter writer = Files.newBufferedWriter(sampleSheetPath);
        writer.write(sampleSheet);
        writer.close();

        // get the min trimmed read length
        int minTrimmedReadLength = bclParameters.index1Length;
        if (bclParameters.hasIndex2 && input.sampleSpecSetHasIndex2) {
            minTrimmedReadLength = Math.min(bclParameters.index1Length, bclParameters.index2Length);
        }

        // determine the number of threads to use
        int pthreads = input.processingThreadNumber;
        int iothreads = pthreads / 3;
        if (iothreads == 0) { iothreads = 1; }

        // define the process
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(
                "bcl2fastq",
                "--min-log-level", "WARNING",
                "--minimum-trimmed-read-length", Integer.toString(minTrimmedReadLength),
                "--mask-short-adapter-reads", Integer.toString(minTrimmedReadLength),
                "--create-fastq-for-index-reads",
                "--ignore-missing-positions",
                "--ignore-missing-filter",
                "--ignore-missing-bcls",
                "--runfolder-dir", input.bclPath.toString(),
                "--sample-sheet", sampleSheetPath.toString(),
                "--interop-dir", interopDirPath.toString(),
                "--stats-dir", statsDirPath.toString(),
                "--reports-dir", reportsDirPath.toString(),
                "--output-dir", outputDirPath.toString(),
                "-p", Integer.toString(pthreads),
                "-r", Integer.toString(iothreads),
                "-w", Integer.toString(iothreads)
        );
        builder.inheritIO();

        // run the bcl2fastq process
        Process process = builder.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) { throw new RuntimeException("bcl2fastq return a non-zero exit code"); }

        return outputDirPath;
    }

    private static Map<String, Map<String, Fastq>> getMasterFastqByReadTypeByLaneStr(Path bcl2fastqOutputDirPath)
            throws IOException {

        // get the paths of the master fastqs
        File dirFile = bcl2fastqOutputDirPath.toFile();
        File[] fastqFiles = dirFile.listFiles(Fastq.getFastqFilenameFilter());

        if (fastqFiles == null) {
            throw new RuntimeException("bcl2fastq output dir path does not appear to be an existant dir, "
                    + bcl2fastqOutputDirPath.toString());
        }

        // create the master fastq by read type by lane str map
        Map<String, Map<String, Fastq>> resultMap = new HashMap<>();
        for (File file : fastqFiles) {

            Fastq fastq = new Fastq(file.toPath());

            if (!resultMap.containsKey(fastq.laneStr)) {
                resultMap.put(fastq.laneStr, new HashMap<>());
            }

            if (resultMap.get(fastq.laneStr).containsKey(fastq.readTypeStr)) {
                throw new RuntimeException("duplicate read types found, " + fastq.filename);
            }

            resultMap.get(fastq.laneStr).put(fastq.readTypeStr, fastq);
        }

        // make sure all the fastqs for a lane have the same first read id
        for (String laneStr : resultMap.keySet()) {

            Set<String> firstReadIdSet = new HashSet<>();
            for (Fastq fastq : resultMap.get(laneStr).values()) {
                firstReadIdSet.add(fastq.getFirstReadID());
            }

            if (firstReadIdSet.size() != 1) {
                throw new RuntimeException("master fastq files for lane " + laneStr + " had different first read ids");
            }
        }

        // make sure all lanes have the same set of read types
        Set<String> readTypeSet = null;
        for (String laneStr : resultMap.keySet()) {

            if (readTypeSet == null) {
                readTypeSet = new HashSet<>(resultMap.get(laneStr).keySet());

            } else {
                if (!readTypeSet.equals(new HashSet<>(resultMap.get(laneStr).keySet()))) {
                    throw new RuntimeException("different lanes of the master fastqs had different read types");
                }
            }
        }

        // create an unmodifiable view
        resultMap.replaceAll((k,v)->Collections.unmodifiableMap(v));
        resultMap = Collections.unmodifiableMap(resultMap);

        return resultMap;
    }

    private static Map<Integer, String> getLaneStrByLaneInt(
            Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr) {

        return masterFastqByReadTypeByLaneStr.keySet().stream()
                .collect(Collectors.toUnmodifiableMap(Fastq::getLaneIntFromLaneStr, Function.identity()));
    }

    private static Map<String, Set<SampleIndexSpec>> getSampleIndexSpecSetByLaneStr(
            Set<SampleIndexSpec> sampleIndexSpecSet, Map<Integer, String> laneStrByLaneInt) {

        // create the sample index spec map
        Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr = new HashMap<>();
        for (SampleIndexSpec sampleIndexSpec : sampleIndexSpecSet) {

            // get the lane string for this sample index's lane int
            String laneStr = laneStrByLaneInt.get(sampleIndexSpec.lane);

            // add set to map if necessary
            if (!sampleIndexSpecSetByLaneStr.containsKey(laneStr)) {
                sampleIndexSpecSetByLaneStr.put(laneStr, new HashSet<>());
            }

            // add sample index spec to map
            sampleIndexSpecSetByLaneStr.get(laneStr).add(sampleIndexSpec);
        }

        // set unmodifiable views of the sample maps on this context intance
        sampleIndexSpecSetByLaneStr.replaceAll((k,v)->Collections.unmodifiableSet(v));

        return Collections.unmodifiableMap(sampleIndexSpecSetByLaneStr);
    }

    private static boolean determineIndex2ReverseCompliment(
            Input input, BCLParameters bclParameters, Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
            Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr
    ) throws IOException {

        // check input
        if (!input.sampleSpecSetHasIndex2 || !bclParameters.hasIndex2) { return false; }

        // make sure that the index 2 read type is in each lane
        if (!masterFastqByReadTypeByLaneStr.values().stream().allMatch(v->v.containsKey(Fastq.INDEX_2_READ_TYPE_STR))) {
            throw new RuntimeException("master fastq map doesn't have an index 2 read type in each lane");
        }

        // now sample reads from each lane
        Map<String, Integer> undeterminedCountByIndexStr = new HashMap<>();
        Map<String, Integer> forwardFindCountByIndexStr = new HashMap<>();
        Map<String, Integer> revCompFindCountByIndexStr = new HashMap<>();
        for (String laneStr : masterFastqByReadTypeByLaneStr.keySet()) {

            // get the sample index key mapping collection
            SampleIndexKeyMappingCollection sampleIndexKeyMappingCollection = new SampleIndexKeyMappingCollection(
                    sampleIndexSpecSetByLaneStr.get(laneStr),
                    bclParameters.index1Length,
                    bclParameters.index2Length
            );

            // get the sample index lookups
            SampleIndexLookup forwardLookup = new SampleIndexLookup(
                    sampleIndexKeyMappingCollection, false
            );
            SampleIndexLookup revCompLookup = new SampleIndexLookup(
                    sampleIndexKeyMappingCollection, false
            );

            // get the index fastqs
            Fastq index1Fastq = masterFastqByReadTypeByLaneStr.get(laneStr).get(Fastq.INDEX_1_READ_TYPE_STR);
            Fastq index2Fastq = masterFastqByReadTypeByLaneStr.get(laneStr).get(Fastq.INDEX_2_READ_TYPE_STR);

            // create index fastq readers
            BufferedReader index1Reader = Utils.getBufferedGzippedFileReader(index1Fastq.path);
            BufferedReader index2Reader = Utils.getBufferedGzippedFileReader(index2Fastq.path);

            // read the first 1,000,000 sequences
            for (int i = 0; i < 1000000; i++) {

                // read lines
                SequenceLines index1SequenceLines = new SequenceLines(
                        index1Reader.readLine(),
                        index1Reader.readLine(),
                        index1Reader.readLine(),
                        index1Reader.readLine()
                );

                SequenceLines index2SequenceLines = new SequenceLines(
                        index2Reader.readLine(),
                        index2Reader.readLine(),
                        index2Reader.readLine(),
                        index2Reader.readLine()
                );

                // break if we are done reading
                if (index1SequenceLines.line1 == null) {
                    if (!index1SequenceLines.allLinesAreNull() || !index2SequenceLines.allLinesAreNull()) {
                        throw new RuntimeException(
                                "read incomplete sets of lines from index fastqs for lane " + laneStr
                        );
                    }
                    break;
                }

                // get the index string to record the count
                String indexStr = index1SequenceLines.line2 + "-" + index2SequenceLines.line2;

                // get the lookup results
                String forwardResult = forwardLookup.lookupProjectSampleId(
                        index1SequenceLines.line2, index2SequenceLines.line2
                );

                String revCompResult = revCompLookup.lookupProjectSampleId(
                        index1SequenceLines.line2, index2SequenceLines.line2
                );

                // record the lookup results
                if (forwardResult == null && revCompResult == null) {

                    if (!undeterminedCountByIndexStr.containsKey(indexStr)) {
                        undeterminedCountByIndexStr.put(indexStr, 1);

                    } else {
                        undeterminedCountByIndexStr.put(indexStr, undeterminedCountByIndexStr.get(indexStr) + 1);
                    }
                }

                if (forwardResult != null) {

                    if (!forwardFindCountByIndexStr.containsKey(indexStr)) {
                        forwardFindCountByIndexStr.put(indexStr, 1);

                    } else {
                        forwardFindCountByIndexStr.put(indexStr, forwardFindCountByIndexStr.get(indexStr) + 1);
                    }
                }

                if (revCompResult != null) {

                    if (!revCompFindCountByIndexStr.containsKey(indexStr)) {
                        revCompFindCountByIndexStr.put(indexStr, 1);

                    } else {
                        revCompFindCountByIndexStr.put(indexStr, revCompFindCountByIndexStr.get(indexStr) + 1);
                    }
                }
            }
        }

        // get total counts
        int forwardFindCount = forwardFindCountByIndexStr.values().stream().reduce(0, Integer::sum);
        int revCompFindCount = revCompFindCountByIndexStr.values().stream().reduce(0, Integer::sum);
        int totalfindCount = forwardFindCount + revCompFindCount;

        // check the signal
        int majorFindCount = Math.max(forwardFindCount, revCompFindCount);
        double majorToTotalRatio = majorFindCount / (double) totalfindCount;

        // throw error if the major to total ratio is insufficient
        if (majorToTotalRatio < .8) {

            // create error message
            StringBuilder errorStringBuilder = new StringBuilder();

            errorStringBuilder.append(
                    "ratio of greater find count to total was less than .8, sequence orientation of index 2 is " +
                            "ambiguous\n"
            );

            errorStringBuilder.append("Undetermined Index Str Counts\n");

            undeterminedCountByIndexStr.keySet().stream()
                    .sorted(Comparator.comparingInt(a -> -undeterminedCountByIndexStr.get(a)))
                    .collect(Collectors.toList())
                    .subList(0, 5)
                    .forEach(s -> errorStringBuilder.append(s).append(": ").append(undeterminedCountByIndexStr.get(s)).append("\n"));

            errorStringBuilder.append("Forward Index Str Counts\n");

            forwardFindCountByIndexStr.keySet().stream()
                    .sorted(Comparator.comparingInt(a -> -forwardFindCountByIndexStr.get(a)))
                    .collect(Collectors.toList())
                    .subList(0, 5)
                    .forEach(s -> errorStringBuilder.append(s).append(": ").append(forwardFindCountByIndexStr.get(s)).append("\n"));

            errorStringBuilder.append("Reverse Compliment Index Str Counts\n");

            revCompFindCountByIndexStr.keySet().stream()
                    .sorted(Comparator.comparingInt(a -> -revCompFindCountByIndexStr.get(a)))
                    .collect(Collectors.toList())
                    .subList(0, 5)
                    .forEach(s -> errorStringBuilder.append(s).append(": ").append(revCompFindCountByIndexStr.get(s)).append("\n"));

            throw new RuntimeException(errorStringBuilder.toString());
        }

        return revCompFindCount > forwardFindCount;
    }

    private static DemultiplexingContext determineDemultiplexingContext(
            Input input,
            BCLParameters bclParameters,
            Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
            Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr,
            boolean index2ReverseCompliment
    ) throws IOException {

        // create the output dirs
        Path demultiplexedFastqsDirPath = input.workdirPath.resolve("demultiplexed-fastqs");
        Files.createDirectory(demultiplexedFastqsDirPath);
        Path indexCountsDirPath = input.workdirPath.resolve("index-counts");
        Files.createDirectory(indexCountsDirPath);

        // determine the index 2 length
        int index2Length = 0;
        if (input.sampleSpecSetHasIndex2 && bclParameters.hasIndex2) { index2Length = bclParameters.index2Length; }

        // create the context of this demultiplexing process
        return new DemultiplexingContext(
                masterFastqByReadTypeByLaneStr,
                sampleIndexSpecSetByLaneStr,
                bclParameters.index1Length,
                index2Length,
                index2ReverseCompliment,
                demultiplexedFastqsDirPath,
                indexCountsDirPath
        );
    }

    private static Map<String, Map<String, Map<String, Long>>> demultiplexMasterFastqs(
            Input input, DemultiplexingContext demultiplexingContext) throws IOException, InterruptedException {

        // get a set of ids for demultiplexing threads
        Set<Integer> demultiplexingThreadIdSet = new HashSet<>();
        for (int i = 1; i <= input.processingThreadNumber; i++) {

            demultiplexingThreadIdSet.add(i);
        }

        // create a sequence group flow
        SequenceGroupFlow sequenceGroupFlow = new SequenceGroupFlow(
                new HashSet<>(demultiplexingContext.masterFastqByReadTypeByLaneStr.keySet()),
                demultiplexingThreadIdSet
        );

        // create the demultiplexing threads
        ArrayList<DemultiplexingThread> demultiplexingThreadList = new ArrayList<>();
        for (int i : demultiplexingThreadIdSet) {

            demultiplexingThreadList.add(
                    new DemultiplexingThread(i, sequenceGroupFlow, demultiplexingContext)
            );
        }

        // create the reader and writer threads
        ArrayList<ReaderThread> readerThreadList = new ArrayList<>();
        ArrayList<WriterThread> writerThreadList = new ArrayList<>();
        for (String laneStr : demultiplexingContext.masterFastqByReadTypeByLaneStr.keySet()) {

            readerThreadList.add(new ReaderThread(laneStr, sequenceGroupFlow, demultiplexingContext));
            writerThreadList.add(new WriterThread(laneStr, sequenceGroupFlow, demultiplexingContext));
        }

        // start threads
        for (ReaderThread readerThread : readerThreadList) { readerThread.start(); }
        for (DemultiplexingThread demultiplexingThread : demultiplexingThreadList) { demultiplexingThread.start(); }
        for (WriterThread writerThread : writerThreadList) { writerThread.start(); }

        // wait on the writer threads which should be the last to complete
        for (WriterThread writerThread : writerThreadList) { writerThread.join(); }

        // return the count by index string by id by lane string map
        return sequenceGroupFlow.getCountByIndexStrByIdByLaneStr();
    }

    private static void writeIndexCounts(
            DemultiplexingContext demultiplexingContext,
            Map<String, Map<String, Map<String, Long>>> countByIndexStrByIdByLaneStr
    ) throws IOException {

        for (String laneStr : countByIndexStrByIdByLaneStr.keySet()) {

            // create the directory for the lane
            Path laneStrDirPath = demultiplexingContext.indexCountsDirPath.resolve(laneStr);
            Files.createDirectory(laneStrDirPath);

            for (String id : countByIndexStrByIdByLaneStr.get(laneStr).keySet()) {

                // get count by index string
                Map<String, Long> countByIndexStr = countByIndexStrByIdByLaneStr.get(laneStr).get(id);

                // get sorted index string list
                List<String> sortedIndexStrList = countByIndexStr.keySet().stream()
                        .filter(v->countByIndexStr.get(v) > 100)
                        .sorted(Comparator.comparingLong(k -> -countByIndexStr.get(k)))
                        .collect(Collectors.toList());

                // get file path for this sample id
                Path outputFilePath = laneStrDirPath.resolve(id + ".tsv");

                // open, write, and close the file
                try (
                        BufferedWriter writer = new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream(outputFilePath.toString()))
                        )
                ) {

                    // write out each line
                    for (String indexStr : sortedIndexStrList) {

                        writer.write(indexStr + "\t" + countByIndexStr.get(indexStr));
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static void writeTotalCounts(
            DemultiplexingContext demultiplexingContext,
            Map<String, Map<String, Map<String, Long>>> countByIndexStrByIdByLaneStr
    ) throws IOException {

        // get total counts by id
        Map<String, Long> totalCountById = new HashMap<>();
        for (String laneStr : countByIndexStrByIdByLaneStr.keySet()) {

            // for each id in this lane
            for (String id : countByIndexStrByIdByLaneStr.get(laneStr).keySet()) {

                // add an entry for this id if necessary
                if (!totalCountById.containsKey(id)) { totalCountById.put(id, 0L); }

                // add all the index counts for this lane and id
                for (Long count : countByIndexStrByIdByLaneStr.get(laneStr).get(id).values()) {

                    totalCountById.put(id, totalCountById.get(id) + count);
                }
            }
        }

        // get all the non-undetermined ids
        Set<String> nonUndeterminedIdSet = totalCountById.keySet().stream()
                .filter(v->!v.equals(DemultiplexingContext.UNDETERMINED_ID))
                .collect(Collectors.toSet());

        // open, write, and close the file
        Path outputFilePath = demultiplexingContext.indexCountsDirPath.resolve("total-counts.tsv");
        try (
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(outputFilePath.toString())
                        )
                )
        ) {
            // write the undetermined total
            writer.write(
                    DemultiplexingContext.UNDETERMINED_ID
                            + "\t"
                            + totalCountById.get(DemultiplexingContext.UNDETERMINED_ID)
            );
            writer.newLine();

            // write the non-undetermined totals
            for (String id : nonUndeterminedIdSet) {

                writer.write(id + "\t" + totalCountById.get(id));
                writer.newLine();
            }
        }
    }
}
