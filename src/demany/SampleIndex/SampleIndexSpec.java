package demany.SampleIndex;

import org.json.simple.JSONObject;

public class SampleIndexSpec {

    public static SampleIndexSpec fromJSON(JSONObject jsonObject) throws Exception {

        try {
            String project = jsonObject.get("project").toString();
            String sample = jsonObject.get("sample").toString();
            String index1 = jsonObject.get("index1").toString();

            String id = project + "-" + sample;

            String index2 = null;
            if (jsonObject.get("index2") != null) { index2 = jsonObject.get("index2").toString(); }

            int lane = Integer.parseInt(jsonObject.get("lane").toString());

            return new SampleIndexSpec(id, project, sample, index1, index2, lane);

        } catch (Exception e) {
            throw new Exception(String.format("Could not parse sample index from JSON: %s", jsonObject.toJSONString()));
        }
    }

    public final String id;
    public final String project;
    public final String sample;
    public final String index1;
    public final String index2;
    public final int lane;

    public SampleIndexSpec(String id, String project, String sample, String index1, String index2, int lane) {

        // check input
        if (id == null || id.isEmpty()) { throw new RuntimeException("id can be neither null nor empty"); }
        if (project == null || project.isEmpty()) {
            throw new RuntimeException("project can be neither null nor empty");
        }
        if (sample == null || sample.isEmpty()) {
            throw new RuntimeException("sample can be neither null nor empty");
        }
        if (index1 == null || index1.isEmpty()) {
            throw new RuntimeException("index1 can be neither null nor empty");
        }
        if (index2 != null && index2.isEmpty()) {
            throw new RuntimeException("index2 can be null or not empty");
        }
        if (lane < 1) {
            throw new RuntimeException("lane must be greater than 1");
        }

        this.id = id;
        this.project = project;
        this.sample = sample;
        this.index1 = index1;
        this.index2 = index2;
        this.lane = lane;
    }

    @Override
    public boolean equals(Object object) {

        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        SampleIndexSpec other = (SampleIndexSpec) object;

        if (lane != other.lane) return false;
        if (!project.equals(other.project)) return false;
        if (!sample.equals(other.sample)) return false;
        if (!index1.equals(other.index1)) return false;

        boolean index2IsNull = index2 == null;

        if (index2IsNull) { return other.index2 == null; }
        else { return index2.equals(other.index2); }
    }

    @Override
    public int hashCode() {
        int result = project.hashCode();
        result = 31 * result + sample.hashCode();
        result = 31 * result + index1.hashCode();
        result = 31 * result + lane;
        result = 31 * result + (index2 == null ? 0 : index2.hashCode());

        return result;
    }

    public boolean hasIndex2() { return this.index2 != null; }
}
