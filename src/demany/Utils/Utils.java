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

    public static FileOutputStream getFileOutputStream(Path path) throws IOException {
        return new FileOutputStream(path.toString());
    }

    public static BufferedWriter getBufferedGzippedByteArrayWriter(int initialByteSize) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(initialByteSize);
        OutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        Writer writer = new OutputStreamWriter(gzipOutputStream);
        return new BufferedWriter(writer);
    }

    public static void tryToSleep() {
        try { Thread.sleep(6); }
        catch (InterruptedException e) { throw new RuntimeException("could not sleep: " + e.getMessage()); }
    }
}
