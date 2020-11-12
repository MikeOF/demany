package demany.Threading;

import demany.Context;
import demany.DataFlow.FastqWriterGroup;
import demany.DataFlow.SequenceGroup;
import demany.DataFlow.SequenceGroupFlow;
import demany.Utils.Fastq;
import demany.Utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class WriterThread extends Thread {

    private final String laneStr;
    private final SequenceGroupFlow sequenceGroupFlow;
    private final Context context;
    private final HashMap<String, FastqWriterGroup> fastqWriterGroupById = new HashMap<>();

    public WriterThread(String laneStr, SequenceGroupFlow sequenceGroupFlow, Context context) throws IOException {

        this.laneStr = laneStr;
        this.sequenceGroupFlow = sequenceGroupFlow;
        this.context = context;

        // add an undetermined fastq writer group for this lane
        this.fastqWriterGroupById.put(
                Context.undeterminedId,
                new FastqWriterGroup(getUndeterminedOutputFastqByReadType())
        );

        // add a fastq writer group for each sample in this lane
        for (Context.SampleIdData sampleIdData : this.context.sampleIdDataSetByLaneStr.get(laneStr)) {

            this.fastqWriterGroupById.put(
                    sampleIdData.id,
                    new FastqWriterGroup(getSampleOutputFastqByReadType(sampleIdData))
            );
        }

    }

    private HashMap<String, Fastq> getUndeterminedOutputFastqByReadType() {

        HashMap<String, Fastq> resultMap = new HashMap<>();

        for (String readTypeStr : this.context.readTypeStrSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getUndeterminedFastqAtDir(this.context.outputDirPath, this.laneStr, readTypeStr)
            );
        }

        return resultMap;
    }

    private HashMap<String, Fastq> getSampleOutputFastqByReadType(Context.SampleIdData sampleIdData) throws IOException {

        // create the dir for this sample
        Path sampleOutputDir = this.context.outputDirPath.resolve(sampleIdData.project).resolve(sampleIdData.sample);
        Files.createDirectories(sampleOutputDir);

        // create the fastq by id map
        HashMap<String, Fastq> resultMap = new HashMap<>();

        // add a fastq to the map for each read type
        for (String readTypeStr : this.context.readTypeStrSet) {

            resultMap.put(
                    readTypeStr,
                    Fastq.getSampleFastqAtDir(sampleOutputDir, sampleIdData.sample, this.laneStr, readTypeStr)
            );
        }

        return resultMap;
    }

    @Override
    public void run() {

        long sleepMilliseconds = 100;

        while(true) {
            boolean didWork = false;

            // attempt to take a collection of sequence groups
            HashMap<String, SequenceGroup> sequenceGroupById =
                    this.sequenceGroupFlow.takeDemultiplexedSequenceGroups(this.laneStr);

            // write sequences if we got em
            if (sequenceGroupById != null) {

                for (String id : sequenceGroupById.keySet()) {

                    try {
                        this.fastqWriterGroupById.get(id).writeSequences(sequenceGroupById.get(id));
                    } catch (IOException e) {
                        throw new RuntimeException("could not write sequences: " + e.getMessage());
                    }
                }

                didWork = true;
            }

            if (!didWork) {

                // sleep a bit
                Utils.tryToSleep(sleepMilliseconds);

                // check to see if we are finished
                if (this.sequenceGroupFlow.allReadThreadsFinished() &&
                        this.sequenceGroupFlow.allDemultiplexingThreadsFinished() &&
                        !this.sequenceGroupFlow.moreDemultiplexedSequenceGroupsAvailable()) {

                    break;
                }

                // increase sleep milliseconds
                sleepMilliseconds = (sleepMilliseconds + 10);

            } else {

                // decrease sleep milliseconds
                if (sleepMilliseconds > 20) { sleepMilliseconds = sleepMilliseconds - 10; }
            }
        }
    }
}
