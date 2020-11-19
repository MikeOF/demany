package demany.Fastq;

import demany.Utils.Utils;

import java.io.BufferedReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Fastq {

    public static final String INDEX_1_READ_TYPE_STR = "I1";
    public static final String INDEX_2_READ_TYPE_STR = "I2";
    public static final String SAMPLE_1_STR = "S1";
    public static final String STANDARD_TAIL = "001.fastq.gz";

    public static final Pattern undeterminedFastqPattern = Pattern.compile(
            "^(undetermined)_(S[0-9]+)_(L[0-9]*[1-9]+)_([RI][1-9]+[0-9]*)_(001\\.fastq\\.gz)$",
            Pattern.CASE_INSENSITIVE
    );

    public static final Pattern sampleFastqPattern = Pattern.compile(
            "^([A-Z0-9-_]+)_(S[0-9]+)_(L[0-9]*[1-9]+)_([RI][1-9]+[0-9]*)_(001\\.fastq\\.gz)$",
            Pattern.CASE_INSENSITIVE
    );

    public static final Pattern indexReadTypePattern = Pattern.compile(
            "^I[1-9]+[0-9]*$",
            Pattern.CASE_INSENSITIVE
    );

    public static Fastq getSampleFastqAtDir(Path dirPath, String sample, String laneStr, String readTypeStr) {

        checkParentDirPath(dirPath);

        return new Fastq(dirPath.resolve(getFilenameForSampleFastq(sample, laneStr, readTypeStr)));
    }

    public static Fastq getUndeterminedFastqAtDir(Path dirPath, String laneStr, String readTypeStr) {

        checkParentDirPath(dirPath);

        return new Fastq(dirPath.resolve(getFilenameForUndeterminedFastq(laneStr, readTypeStr)));
    }

    public static FilenameFilter getFastqFilenameFilter() {
        return (dir, name) -> name.toLowerCase().endsWith("fastq.gz");
    }

    public static int getLaneIntFromLaneStr(String laneStr) {

        Matcher matcher = Fastq.laneStrPattern.matcher(laneStr);

        if (!matcher.matches()) {
            throw new RuntimeException("could not match the lane str: " + laneStr);
        }

        return Integer.parseInt(matcher.group(3));
    }

    private static void checkParentDirPath(Path dirPath) {

        // check path
        if (!dirPath.isAbsolute()) {
            throw new RuntimeException("dirPath must be absolute");
        }

        if (!Files.isDirectory(dirPath)) {
            throw new RuntimeException("dirPath must be an existant directory");
        }
    }

    private static final Pattern laneStrPattern = Pattern.compile("(L)([0]*)([0-9]*)", Pattern.CASE_INSENSITIVE);

    public final Path path;
    public final String filename;
    private final boolean isUndetermined;
    public final String name;
    public final String sampleNumber;
    public final String laneStr;
    public final String readTypeStr;
    public final String tail;
    public final boolean isAnIndexFastq;

    public Fastq(Path path) throws RuntimeException {

        // set file properties
        this.path = path.toAbsolutePath();
        this.filename = path.getFileName().toString();

        // determined if it is a sample fastq or an undetermiend fastq
        Matcher undeterminedMatcher = undeterminedFastqPattern.matcher(filename);
        Matcher sampleMatcher = sampleFastqPattern.matcher(filename);

        // check matches
        if (!undeterminedMatcher.matches() && !sampleMatcher.matches()) {
            throw new RuntimeException("cannot be niether undetermiend nor sample fastq: " + filename);
        }

        isUndetermined = undeterminedMatcher.matches();

        // set fastq properties
        Matcher matcher = undeterminedMatcher.matches() ? undeterminedMatcher : sampleMatcher;

        this.name = matcher.group(1);
        this.sampleNumber = matcher.group(2);
        this.laneStr = matcher.group(3);
        this.readTypeStr = matcher.group(4);
        this.tail = matcher.group(5);

        // determine if this is an index fastq
        this.isAnIndexFastq = indexReadTypePattern.matcher(this.readTypeStr).matches();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        Fastq other = (Fastq) object;

        return path.equals(other.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    private static String getFilenameForSampleFastq(String sample, String laneStr, String readTypeStr) {
        return sample + "_" + SAMPLE_1_STR + "_" + laneStr + "_" + readTypeStr + "_" + STANDARD_TAIL;
    }

    private static String getFilenameForUndeterminedFastq(String laneStr, String readTypeStr) {
        return "Undetermined_S0_" + laneStr + "_" + readTypeStr + "_" + STANDARD_TAIL;
    }

    public boolean isAnUndeterminedFastq() {
        return isUndetermined;
    }

    public boolean isASampleFastq() {
        return !isUndetermined;
    }

    public String getFirstReadID() throws IOException {

        BufferedReader reader = Utils.getBufferedGzippedFileReader(path);

        String firstLine = reader.readLine();

        reader.close();

        return firstLine.strip().split("\\s+")[0];
    }
}
