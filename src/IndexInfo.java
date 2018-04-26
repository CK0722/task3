import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * data structure describing index information
 */
public class IndexInfo implements Serializable {
    private static final long serialVersionUID = -3284879889186446978L;

    /**
     * the specified value
     */
    private String value;

    /**
     * a set records the index information
     */
    private List<IndexItem> items;

    public IndexInfo() {
        items = new ArrayList<>(16);
    }

    public IndexInfo(String value, int page, int row) {
        items = new ArrayList<>(16);
        this.value = value;
        this.addItem(page, row);
    }

    public void addItem(int page, int row) {
        IndexItem item = new IndexItem(page, row);
        this.items.add(item);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<IndexItem> getItems() {
        return items;
    }

    public void setItems(List<IndexItem> items) {
        this.items = items;
    }

    public static class IndexItem implements Serializable {
        private static final long serialVersionUID = -2410945364046324694L;

        /**
         * the page number of a specified value in the dataset
         */
        private int pageNum;

        /**
         * the row number of a specified value in a specified page in the dataset
         */
        private int row;

        public IndexItem(int pageNum, int row) {
            this.pageNum = pageNum;
            this.row = row;
        }

        public static long getSerialVersionUID() {
            return serialVersionUID;
        }

        public int getPageNum() {
            return pageNum;
        }

        public void setPageNum(int pageNum) {
            this.pageNum = pageNum;
        }

        public int getRow() {
            return row;
        }

        public void setRow(int row) {
            this.row = row;
        }
    }
}
