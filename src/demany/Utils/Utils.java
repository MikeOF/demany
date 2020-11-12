package demany.Utils;

import java.io.*;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Utils {

    public static BufferedReader getBufferedGzippedFileReader(Path path) throws IOException {
        InputStream fileInputStream = new FileInputStream(path.toString());
        InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        Reader decoder = new InputStreamReader(gzipInputStream);
        return new BufferedReader(decoder);
    }

    public static BufferedWriter getBufferedGzippedFileWriter(Path path) throws IOException {
        OutputStream fileOutputStream = new FileOutputStream(path.toString());
        OutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);
        Writer encoder = new OutputStreamWriter(gzipOutputStream);
        return new BufferedWriter(encoder);
    }

    public static String getIdForProjectSample(String project, String sample) {
        return project + "-" + sample;
    }

    public static void tryToSleep(long sleepMilliseconds) {
        try {
            Thread.sleep(sleepMilliseconds);
        } catch (InterruptedException e) {
            throw new RuntimeException("could not sleep: " + e.getMessage());
        }
    }
}
