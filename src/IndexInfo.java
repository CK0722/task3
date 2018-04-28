import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * data structure describing index information
 */
public class IndexInfo implements Serializable {
    private static final long serialVersionUID = -3284879889186446978L;
    private static final String POS_SEP = ",";
    private static final String CONTENT_SEP = ":";

    /**
     * the specified value
     */
    private String value;

    /**
     * record the physical address of  data
     */
    private List<Integer> positions;

    public IndexInfo(String entryStr) {
        parseForPosition(entryStr);
    }

    public IndexInfo(int capcity, String value) {
        this.value = value;
        this.positions = new ArrayList<>(capcity);
    }

    public IndexInfo(int capcity, String value, int pos) {
        this.value = value;
        this.positions = new ArrayList<>(capcity);
        this.positions.add(pos);
    }

    public void addPosition(int pos) {
        this.positions.add(pos);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public void parseForPosition(String indexStr) {
        int i = indexStr.lastIndexOf(CONTENT_SEP);
        if (i < 0) {
            return;
        }
        this.value = indexStr.substring(0, i);
        String[] posArrar = indexStr.substring(i + 1).split(POS_SEP);
        this.positions = new ArrayList<>(posArrar.length);
        for (String pos : posArrar) {
            this.positions.add(Integer.parseInt(pos));
        }
    }

    public List<Integer> getPositions() {
        return this.positions;
    }


    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer(this.positions.size() + this.value.length() + CONTENT_SEP.length());
        buffer.append(this.value);
        buffer.append(CONTENT_SEP);
        this.positions.forEach(pos -> {
            buffer.append(pos + POS_SEP);
        });
        buffer.deleteCharAt(buffer.length() - 1);
        return buffer.toString();
    }


}
