package demany;

import demany.DataFlow.SequenceGroupFlow;
import demany.DataFlow.SequenceLines;
import demany.SampleIndex.SampleIndexKeyMappingCollection;
import demany.SampleIndex.SampleIndexLookup;
import demany.SampleIndex.SampleIndexSpec;
import demany.Threading.DemultiplexingThread;
import demany.Threading.ReaderThread;
import demany.Threading.WriterThread;
import demany.Utils.BCLParameters;
import demany.Utils.Fastq;
import demany.Utils.Utils;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Demultiplex {

    private static final Logger LOGGER = Logger.getLogger( Demultiplex.class.getName() );

    static int ExecuteDemultiplex(Input input)
            throws IOException, SAXException, ParserConfigurationException, InterruptedException {

        // print time
        LOGGER.log(Level.INFO,"\n\n ---- Start of Demultiplexing Process ----\n");

        // check input
        if (input.sampleIndexSpecSet.isEmpty()) {
            throw new RuntimeException("sample index spec set is empty");
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

        // determine the index 2 is reverse compliment parameter
        boolean index2ReverseCompliment = determineIndex2ReverseCompliment(
                input, bclParameters, masterFastqByReadTypeByLaneStr
        );

        // determined the demultiplexing context
        Context context = determineDemultiplexingContext(
                input, bclParameters, masterFastqByReadTypeByLaneStr, index2ReverseCompliment
        );

        // demultiplex the master fastqs
        demultiplexMasterFastqs(input, context);

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
        if (bclParameters.hasIndex2) {
            minTrimmedReadLength = Math.min(bclParameters.index1Length, bclParameters.index2Length);
        }

        // determine the number of threads to use
        int pthreads = input.demultiplexingThreadNumber;
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
            throw new RuntimeException("bcl2fastq output dir path does not appear to be an existant dir, " +
                    bcl2fastqOutputDirPath.toString());
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

        // create an unmodifiable view
        resultMap.replaceAll((k,v)->Collections.unmodifiableMap(v));
        resultMap = Collections.unmodifiableMap(resultMap);

        return resultMap;
    }

    private static boolean determineIndex2ReverseCompliment(
            Input input, BCLParameters bclParameters, Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr) throws IOException {

        // check input
        if (!input.sampleSpecSetHasIndex2 || bclParameters.hasIndex2) { return false; }

        // make sure that the index 2 read type is in each lane
        if (!masterFastqByReadTypeByLaneStr.values().stream().allMatch(v->v.containsKey(Fastq.INDEX_2_READ_TYPE_STR))) {
            throw new RuntimeException("master fastq map doesn't have an index 2 read type in each lane");
        }

        // get lane str by lane int
        Map<Integer, String> laneStrByLaneInt = masterFastqByReadTypeByLaneStr.keySet().stream()
                .collect(Collectors.toMap(Fastq::getLaneIntFromLaneStr, Function.identity()));

        // get sample index specs by lane
        Map<String, Set<SampleIndexSpec>> sampleIndexSpecSetByLaneStr = new HashMap<>();
        for (SampleIndexSpec sampleIndexSpec : input.sampleIndexSpecSet) {

            String laneStr = laneStrByLaneInt.get(sampleIndexSpec.lane);
            if (!sampleIndexSpecSetByLaneStr.containsKey(laneStr)) {
                sampleIndexSpecSetByLaneStr.put(laneStr, new HashSet<>());
            }

            sampleIndexSpecSetByLaneStr.get(laneStr).add(sampleIndexSpec);
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
                        revCompFindCountByIndexStr.put(indexStr, forwardFindCountByIndexStr.get(indexStr) + 1);
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

    private static Context determineDemultiplexingContext(
            Input input, BCLParameters bclParameters, Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
            boolean index2ReverseCompliment) throws IOException {

        // create the output dirs
        Path demultiplexedFastqsDirPath = input.workdirPath.resolve("demultiplexed-fastqs");
        Files.createDirectory(demultiplexedFastqsDirPath);
        Path indexCountsDirPath = input.workdirPath.resolve("index-counts");
        Files.createDirectory(indexCountsDirPath);

        // determine the index 2 length
        int index2Length = 0;
        if (input.sampleSpecSetHasIndex2 && bclParameters.hasIndex2) { index2Length = bclParameters.index2Length; }

        // create the context of this demultiplexing process
        return new Context(
                masterFastqByReadTypeByLaneStr,
                input.sampleIndexSpecSet,
                bclParameters.index1Length,
                index2Length,
                index2ReverseCompliment,
                demultiplexedFastqsDirPath,
                input.sequenceChunkSize
        );
    }

    private static void demultiplexMasterFastqs(Input input, Context context) throws IOException, InterruptedException {

        // resolve the number of demultiplexing threads we need
        int numDemultiplexingThreads = input.demultiplexingThreadNumber;
        if (numDemultiplexingThreads == -1) { numDemultiplexingThreads = context.masterFastqByReadTypeByLaneStr.size(); }

        // get a set of ids for demultiplexing threads
        Set<Integer> demultiplexingThreadIdSet = new HashSet<>();
        for (int i = 1; i <= numDemultiplexingThreads; i++) { demultiplexingThreadIdSet.add(i); }

        // create a sequence group flow
        SequenceGroupFlow sequenceGroupFlow = new SequenceGroupFlow(
                new HashSet<>(context.masterFastqByReadTypeByLaneStr.keySet()),
                input.sequenceChunkQueueSize,
                demultiplexingThreadIdSet
        );

        // create the threads
        ArrayList<DemultiplexingThread> demultiplexingThreadList = new ArrayList<>();
        for (int i : demultiplexingThreadIdSet) {
            demultiplexingThreadList.add(new DemultiplexingThread(i, sequenceGroupFlow, context));
        }

        ArrayList<ReaderThread> readerThreadList = new ArrayList<>();
        ArrayList<WriterThread> writerThreadList = new ArrayList<>();
        for (String laneStr : context.masterFastqByReadTypeByLaneStr.keySet()) {

            readerThreadList.add(new ReaderThread(laneStr, sequenceGroupFlow, context));
            writerThreadList.add(new WriterThread(laneStr, sequenceGroupFlow, context));
        }

        // start threads
        for (ReaderThread readerThread : readerThreadList) { readerThread.start(); }
        for (DemultiplexingThread demultiplexingThread : demultiplexingThreadList) { demultiplexingThread.start(); }
        for (WriterThread writerThread : writerThreadList) { writerThread.start(); }

        // wait on the writer threads which should be the last to complete
        for (WriterThread writerThread : writerThreadList) { writerThread.join(); }
    }
}
