package demany;

import demany.DataFlow.SequenceGroupFlow;
import demany.Threading.DemultiplexingThread;
import demany.Threading.ReaderThread;
import demany.Threading.WriterThread;
import demany.Utils.BCLParameters;
import demany.Utils.Fastq;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Demultiplex {

    private static final Logger LOGGER = Logger.getLogger( Demultiplex.class.getName() );

    static int ExecuteDemultiplex(Input input) throws IOException, SAXException, ParserConfigurationException, InterruptedException {

        // print time
        LOGGER.log(Level.INFO,"\n\n ---- Start of Process ----\n");

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

        // determine if index 2 is reverse compliment
        boolean index2ReverseCompliment = false;

        // demultiplex the master fastqs
        demultiplexMasterFastqs(input, bclParameters, masterFastqByReadTypeByLaneStr, index2ReverseCompliment);

        LOGGER.log(Level.INFO,"\n\n ---- End of Process ----\n");

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

        if (bclParameters.hasIndex2) {

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

        LOGGER.log(Level.INFO, "bcl2fastq completed with exit code: {0}", exitCode);

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

        // create and return an unmodifiable view
        resultMap.replaceAll((k,v) -> Collections.unmodifiableMap(resultMap.get(k)));
        return Collections.unmodifiableMap(resultMap);
    }

    private static void demultiplexMasterFastqs(Input input, BCLParameters bclParameters,
                                                Map<String, Map<String, Fastq>> masterFastqByReadTypeByLaneStr,
                                                boolean index2ReverseCompliment) throws IOException, InterruptedException {

        // create the output dir
        Path outputDirPath = input.workdirPath.resolve("demultiplexed-fastqs");
        Files.createDirectory(outputDirPath);

        // create the context of this demultiplexing process
        Context context = new Context(
                masterFastqByReadTypeByLaneStr,
                input.sampleIndexSpecSet,
                bclParameters.index1Length,
                bclParameters.index2Length,
                index2ReverseCompliment,
                outputDirPath,
                input.sequenceChunkSize
        );

        // resolve the number of demultiplexing threads we need
        int numDemultiplexingThreads = input.demultiplexingThreadNumber;
        if (numDemultiplexingThreads == -1) { numDemultiplexingThreads = masterFastqByReadTypeByLaneStr.size(); }

        // get a set of ids for demultiplexing threads
        Set<Integer> demultiplexingThreadIdSet = new HashSet<>();
        for (int i = 1; i <= numDemultiplexingThreads; i++) { demultiplexingThreadIdSet.add(i); }

        // create a sequence group flow
        SequenceGroupFlow sequenceGroupFlow = new SequenceGroupFlow(
                new HashSet<>(masterFastqByReadTypeByLaneStr.keySet()),
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
