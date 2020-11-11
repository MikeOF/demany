package demany.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Fastq {

    public static final String INDEX_1_READ_TYPE_STR = "I1";
    public static final String INDEX_2_READ_TYPE_STR = "I2";
    public static final String SAMPLE_1_STR = "S1";

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

    public static HashMap<String, Fastq> mapFastqsByReadType(Set<Fastq> fastqSet) {

        HashMap<String, Fastq> resultMap = new HashMap<>(fastqSet.size());

        for (Fastq fastq : fastqSet) {

            if (resultMap.containsKey(fastq.readTypeStr)) {
                throw new RuntimeException("two fastqs have the same read type string");
            }

            resultMap.put(fastq.readTypeStr, fastq);
        }

        return resultMap;
    }

    public static ArrayList<HashSet<Fastq>> groupFastqsByFirstReadId(Set<Fastq> fastqSet) throws IOException {

        // first group the fastqs
        HashMap<String, HashSet<Fastq>> fastqSetByFirstReadId = new HashMap<>();

        for (Fastq fastq : fastqSet) {

            String firstReadId = fastq.getFirstReadID();

            if (!fastqSetByFirstReadId.containsKey(firstReadId)) {
                fastqSetByFirstReadId.put(firstReadId, new HashSet<>());
            }

            fastqSetByFirstReadId.get(firstReadId).add(fastq);
        }

        // now convert the map to an array and return
        return new ArrayList<>(fastqSetByFirstReadId.values());
    }

    public static ArrayList<HashMap<String, Fastq>> groupFastqsByReadTypeByFirstReadId(Set<Fastq> fastqSet) throws IOException {

        // first group the fastqs by readId
        ArrayList<HashSet<Fastq>> fastqForReadIdSetList = groupFastqsByFirstReadId(fastqSet);

        // now group those group by their read types
        ArrayList<HashMap<String, Fastq>> resultList = new ArrayList<>();
        for (Set<Fastq> fastqSetForReadId : fastqForReadIdSetList) {

            resultList.addAll(groupFastqsByReadTypeByFirstReadId(fastqSetForReadId));
        }

        return resultList;
    }
}
