package demany.Utils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FastqTest {

    @Test
    void testSampleFastq() {

        Fastq fastq;

        fastq = new Fastq(Path.of("200103_A00521_0143_AHWY2CDSXX/PM067F_S1_L004_I1_001.fastq.gz"));

        assertFalse(fastq.isAnUndeterminedFastq());
        assertTrue(fastq.isASampleFastq());
        assertTrue(fastq.isAnIndexFastq);
        assertEquals(
                Path.of("200103_A00521_0143_AHWY2CDSXX/PM067F_S1_L004_I1_001.fastq.gz").toAbsolutePath(),
                fastq.path
        );
        assertEquals("PM067F_S1_L004_I1_001.fastq.gz", fastq.filename);
        assertEquals("PM067F", fastq.name);
        assertEquals("S1", fastq.sampleNumber);
        assertEquals("L004", fastq.laneStr);
        assertEquals("I1", fastq.readTypeStr);
        assertEquals("001.fastq.gz", fastq.tail);

        fastq = new Fastq(Path.of("200103_A00521_0143_AHWY2CDSXX/PM067F_S1_L004_R1_001.fastq.gz"));

        assertFalse(fastq.isAnUndeterminedFastq());
        assertTrue(fastq.isASampleFastq());
        assertFalse(fastq.isAnIndexFastq);
        assertEquals(
                Path.of("200103_A00521_0143_AHWY2CDSXX/PM067F_S1_L004_R1_001.fastq.gz").toAbsolutePath(),
                fastq.path
        );
        assertEquals("PM067F_S1_L004_R1_001.fastq.gz", fastq.filename);
        assertEquals("PM067F", fastq.name);
        assertEquals("S1", fastq.sampleNumber);
        assertEquals("L004", fastq.laneStr);
        assertEquals("R1", fastq.readTypeStr);
        assertEquals("001.fastq.gz", fastq.tail);
    }

    @Test
    void testUndeterminedFastq() {

        Fastq fastq;

        fastq = new Fastq(Path.of("200103_A00521_0143_AHWY2CDSXX/Undetermined_S0_L003_R1_001.fastq.gz"));

        assertTrue(fastq.isAnUndeterminedFastq());
        assertFalse(fastq.isASampleFastq());
        assertFalse(fastq.isAnIndexFastq);
        assertEquals(
                Path.of("200103_A00521_0143_AHWY2CDSXX/Undetermined_S0_L003_R1_001.fastq.gz").toAbsolutePath(),
                fastq.path
        );
        assertEquals("Undetermined_S0_L003_R1_001.fastq.gz", fastq.filename);
        assertEquals("Undetermined", fastq.name);
        assertEquals("S0", fastq.sampleNumber);
        assertEquals("L003", fastq.laneStr);
        assertEquals("R1", fastq.readTypeStr);
        assertEquals("001.fastq.gz", fastq.tail);

        fastq = new Fastq(Path.of("200103_A00521_0143_AHWY2CDSXX/Undetermined_S0_L003_I1_001.fastq.gz"));

        assertTrue(fastq.isAnUndeterminedFastq());
        assertFalse(fastq.isASampleFastq());
        assertTrue(fastq.isAnIndexFastq);
        assertEquals(
                Path.of("200103_A00521_0143_AHWY2CDSXX/Undetermined_S0_L003_I1_001.fastq.gz").toAbsolutePath(),
                fastq.path
        );
        assertEquals("Undetermined_S0_L003_I1_001.fastq.gz", fastq.filename);
        assertEquals("Undetermined", fastq.name);
        assertEquals("S0", fastq.sampleNumber);
        assertEquals("L003", fastq.laneStr);
        assertEquals("I1", fastq.readTypeStr);
        assertEquals("001.fastq.gz", fastq.tail);
    }
}