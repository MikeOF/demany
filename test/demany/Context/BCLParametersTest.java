package demany.Context;

import demany.Context.BCLParameters;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BCLParametersTest {

    @Test
    void testRunInfoXMLParsing1Index() throws ParserConfigurationException, SAXException, IOException {

        String runInfoXMLString = "<?xml version=\"1.0\"?>\n" +
                "<RunInfo xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" Version=\"4\">\n" +
                "  <Run Id=\"191205_NS500289_0777_AHTW7TBGXC\" Number=\"777\">\n" +
                "    <Flowcell>HTW7TBGXC</Flowcell>\n" +
                "    <Instrument>NS500289</Instrument>\n" +
                "    <Date>191205</Date>\n" +
                "    <Reads>\n" +
                "      <Read Number=\"1\" NumCycles=\"26\" IsIndexedRead=\"N\" />\n" +
                "      <Read Number=\"2\" NumCycles=\"8\" IsIndexedRead=\"Y\" />\n" +
                "      <Read Number=\"3\" NumCycles=\"58\" IsIndexedRead=\"N\" />\n" +
                "    </Reads>\n" +
                "    <FlowcellLayout LaneCount=\"4\" SurfaceCount=\"2\" SwathCount=\"3\" TileCount=\"12\" SectionPerLane=\"3\" LanePerSection=\"2\">\n" +
                "      <TileSet TileNamingConvention=\"FiveDigit\">\n" +
                "        <Tiles>\n" +
                "          <Tile>1_11101</Tile>\n" +
                "        </Tiles>\n" +
                "      </TileSet>\n" +
                "    </FlowcellLayout>\n" +
                "    <ImageDimensions Width=\"2592\" Height=\"1944\" />\n" +
                "    <ImageChannels>\n" +
                "      <Name>Red</Name>\n" +
                "      <Name>Green</Name>\n" +
                "    </ImageChannels>\n" +
                "  </Run>\n" +
                "</RunInfo>";

        Set<BCLParameters.ReadInfo> expectedReadInfoSet = new HashSet<>();
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("1", "26", "N"));
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("2", "8", "Y"));
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("3", "58", "N"));

        Set<BCLParameters.ReadInfo> readInfoSet = BCLParameters.getReadInfoSet(runInfoXMLString);
        boolean hasIndex2 = BCLParameters.getHasIndex2(readInfoSet);
        int index1Length = BCLParameters.getIndex1Length(readInfoSet);
        int index2Length = BCLParameters.getIndex2Length(readInfoSet);

        assertEquals(expectedReadInfoSet, readInfoSet);
        assertFalse(hasIndex2);
        assertEquals(8, index1Length);
        assertEquals(0, index2Length);
    }


    @Test
    void testRunInfoXMLParsing2Index() throws ParserConfigurationException, SAXException, IOException {

        String runInfoXMLString = "<?xml version=\"1.0\"?>\n" +
                "<RunInfo xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" Version=\"4\">\n" +
                "  <Run Id=\"191205_NS500289_0777_AHTW7TBGXC\" Number=\"777\">\n" +
                "    <Flowcell>HTW7TBGXC</Flowcell>\n" +
                "    <Instrument>NS500289</Instrument>\n" +
                "    <Date>191205</Date>\n" +
                "    <Reads>\n" +
                "      <Read Number=\"1\" NumCycles=\"26\" IsIndexedRead=\"N\" />\n" +
                "      <Read Number=\"2\" NumCycles=\"8\" IsIndexedRead=\"Y\" />\n" +
                "      <Read Number=\"3\" NumCycles=\"10\" IsIndexedRead=\"Y\" />\n" +
                "      <Read Number=\"4\" NumCycles=\"58\" IsIndexedRead=\"N\" />\n" +
                "    </Reads>\n" +
                "    <FlowcellLayout LaneCount=\"4\" SurfaceCount=\"2\" SwathCount=\"3\" TileCount=\"12\" SectionPerLane=\"3\" LanePerSection=\"2\">\n" +
                "      <TileSet TileNamingConvention=\"FiveDigit\">\n" +
                "        <Tiles>\n" +
                "          <Tile>1_11101</Tile>\n" +
                "        </Tiles>\n" +
                "      </TileSet>\n" +
                "    </FlowcellLayout>\n" +
                "    <ImageDimensions Width=\"2592\" Height=\"1944\" />\n" +
                "    <ImageChannels>\n" +
                "      <Name>Red</Name>\n" +
                "      <Name>Green</Name>\n" +
                "    </ImageChannels>\n" +
                "  </Run>\n" +
                "</RunInfo>";

        Set<BCLParameters.ReadInfo> expectedReadInfoSet = new HashSet<>();
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("1", "26", "N"));
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("2", "8", "Y"));
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("3", "10", "Y"));
        expectedReadInfoSet.add(new BCLParameters.ReadInfo("4", "58", "N"));

        Set<BCLParameters.ReadInfo> readInfoSet = BCLParameters.getReadInfoSet(runInfoXMLString);
        boolean hasIndex2 = BCLParameters.getHasIndex2(readInfoSet);
        int index1Length = BCLParameters.getIndex1Length(readInfoSet);
        int index2Length = BCLParameters.getIndex2Length(readInfoSet);

        assertEquals(expectedReadInfoSet, readInfoSet);
        assertTrue(hasIndex2);
        assertEquals(8, index1Length);
        assertEquals(10, index2Length);
    }
}