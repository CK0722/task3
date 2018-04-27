import java.io.Serializable;

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
    private StringBuffer position;

    public IndexInfo(int capcity,String value) {
        this.value=value;
        this.position = new StringBuffer(capcity);
    }

    public IndexInfo(int capcity,String value, int pos) {
        this.value=value;
        this.position = new StringBuffer(capcity);
        this.position.append(pos);
    }

    public void addPosition(int pos) {
        this.position.append(POS_SEP + pos);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return this.value + CONTENT_SEP + this.position.toString();
    }


}
