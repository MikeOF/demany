package demany.Utils;

import java.io.*;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

public class Utils {

    public static BufferedReader getBufferedGzippedFileReader(Path path) throws IOException {
        InputStream fileInputStream = new FileInputStream(path.toString());
        InputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        Reader decoder = new InputStreamReader(gzipInputStream);
        return new BufferedReader(decoder);
    }

    public static void tryToSleep() {
        try { Thread.sleep(6); }
        catch (InterruptedException e) { throw new RuntimeException("could not sleep: " + e.getMessage()); }
    }
}
