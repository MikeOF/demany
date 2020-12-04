package demany.Context;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class BCLParameters {

    public static class ReadInfo {

        final int readNumber;
        final int length;
        final boolean isIndexRead;

        ReadInfo(String Number, String NumCycles, String IsIndexedRead) {
            this.readNumber = Integer.parseInt(Number);
            this.length = Integer.parseInt(NumCycles);
            this.isIndexRead = IsIndexedRead.toUpperCase().equals("Y");

        }

        @Override
        public boolean equals(Object boject) {

            if (this == boject) return true;
            if (boject == null || getClass() != boject.getClass()) return false;

            ReadInfo other = (ReadInfo) boject;

            if (this.readNumber != other.readNumber) return false;
            if (this.length != other.length) return false;
            return this.isIndexRead == other.isIndexRead;
        }

        @Override
        public int hashCode() {
            int result = this.readNumber;
            result = 31 * result + this.length;
            result = 31 * result + (this.isIndexRead ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ReadInfo { read number: " + this.readNumber + ", is index read: " + this.isIndexRead
                    + ", length: " + this.length + " }";
        }
    }

    public final Set<ReadInfo> readInfoSet;
    public final boolean hasIndex2;
    public final int index1Length;
    public final int index2Length;

    public BCLParameters(Path bclPath) throws ParserConfigurationException, IOException, SAXException {

        // parse the run info xml file
        Path runInfoPath = bclPath.resolve("RunInfo.xml");

        // read xml into string
        String runInfoXML = Files.readString(runInfoPath);

        // parse the run info
        this.readInfoSet = getReadInfoSet(runInfoXML);

        // mark if we have index 2
        this.hasIndex2 = getHasIndex2(this.readInfoSet);

        // record index length(s)
        this.index1Length = getIndex1Length(this.readInfoSet);
        this.index2Length = getIndex2Length(this.readInfoSet);
    }

    static Set<ReadInfo> getReadInfoSet(String runInfoXML)
            throws IOException, SAXException, ParserConfigurationException {

        // create xml document
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new InputSource(new StringReader(runInfoXML)));

        // get all the read nodes
        NodeList readNodeList = document.getElementsByTagName("Read");

        // create the read info set
        Set<ReadInfo> readInfoSet = new HashSet<>();
        for (int i = 0; i < readNodeList.getLength(); i++) {

            Element element = (Element) readNodeList.item(i);

            readInfoSet.add(
                    new ReadInfo(
                            element.getAttribute("Number"),
                            element.getAttribute("NumCycles"),
                            element.getAttribute("IsIndexedRead")
                    )
            );
        }

        return Collections.unmodifiableSet(readInfoSet);
    }

    static boolean getHasIndex2(Set<ReadInfo> readInfoSet) {

        // determine the number of index reads
        int numberOfIndexReads = readInfoSet.stream().filter(v -> v.isIndexRead).toArray().length;

        if (numberOfIndexReads < 1 || numberOfIndexReads > 2) {
            throw new RuntimeException("the number of index reads must be 1 or 2, was " + numberOfIndexReads);
        }

        return numberOfIndexReads == 2;
    }

    static int getIndex1Length(Set<ReadInfo> readInfoSet) {

        return readInfoSet.stream()
                .filter(v -> v.isIndexRead)
                .min(Comparator.comparingInt(a -> a.readNumber))
                .orElseThrow()
                .length;
    }

    static int getIndex2Length(Set<ReadInfo> readInfoSet) {

        readInfoSet = readInfoSet.stream()
                .filter(v -> v.isIndexRead)
                .collect(Collectors.toSet());

        if (readInfoSet.size() < 2) { return 0; }

        return readInfoSet.stream()
                .max(Comparator.comparingInt(a -> a.readNumber))
                .orElseThrow()
                .length;
    }

    public List<String> getLogLines() {

        List<String> logLines = new ArrayList<>();

        logLines.add("Index 1 Length: " + this.index1Length);
        logLines.add("Index 2 Length: " + this.index2Length);
        logLines.add("Has Index 2: " + this.hasIndex2);
        logLines.add("Read Info Set:");

        for (BCLParameters.ReadInfo readInfo : this.readInfoSet) {
            logLines.add("\t" + readInfo.toString());
        }

        return logLines;
    }
}
