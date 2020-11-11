package demany.DataFlow;

public class SequenceLines {

    public final String line1;
    public final String line2;
    public final String line3;
    public final String line4;

    public SequenceLines(String line1, String line2, String line3, String line4) {

        this.line1 = line1;
        this.line2 = line2;
        this.line3 = line3;
        this.line4 = line4;
    }

    public boolean isNull() {
        // here we are assuming that if line 4 is null then all others are null
        return this.line4 == null;
    }
}
