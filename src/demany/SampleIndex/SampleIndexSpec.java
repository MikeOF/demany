package demany.SampleIndex;

import org.json.simple.JSONObject;

public class SampleIndexSpec {

    public final String id;
    public final String project;
    public final String sample;
    public final String index1;
    public final String index2;
    public final int lane;

    public SampleIndexSpec(JSONObject jsonObject) throws Exception {

        try {
            this.project = jsonObject.get("project").toString();
            this.sample = jsonObject.get("sample").toString();
            this.index1 = jsonObject.get("index1").toString();

            this.id = this.project + "-" + this.sample;

            if (jsonObject.get("index2") == null) {
                this.index2 = null;
            } else {
                this.index2 = jsonObject.get("index2").toString();
            }

            this.lane = Integer.parseInt(jsonObject.get("lane").toString());

        } catch (Exception e) {
            throw new Exception(String.format("Could not parse sample index from JSON: %s", jsonObject.toJSONString()));
        }
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
