/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public interface dbimpl {

    public static final String HEAP_FNAME = "heap.";
    public static final String INDEX_FNAME = "index.";
    public static final String ENCODING = "utf-8";

    // fixed/variable lengths
    public static final int RECORD_SIZE = 297;
    public static final int RID_SIZE = 4;
    public static final int REGISTER_NAME_SIZE = 14;
    public static final int BN_NAME_SIZE = 200;
    public static final int BN_STATUS_SIZE = 12;
    public static final int BN_REG_DT_SIZE = 10;
    public static final int BN_CANCEL_DT_SIZE = 10;
    public static final int BN_RENEW_DT_SIZE = 10;
    public static final int BN_STATE_NUM_SIZE = 10;
    public static final int BN_STATE_OF_REG_SIZE = 3;
    public static final int BN_ABN_SIZE = 20;
    public static final int EOF_PAGENUM_SIZE = 4;

    public static final int BN_NAME_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE;

    public static final int BN_STATUS_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE;

    public static final int BN_REG_DT_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE;

    public static final int BN_CANCEL_DT_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE
            + BN_REG_DT_SIZE;

    public static final int BN_RENEW_DT_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE
            + BN_REG_DT_SIZE
            + BN_CANCEL_DT_SIZE;

    public static final int BN_STATE_NUM_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE
            + BN_REG_DT_SIZE
            + BN_CANCEL_DT_SIZE
            + BN_RENEW_DT_SIZE;

    public static final int BN_STATE_OF_REG_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE
            + BN_REG_DT_SIZE
            + BN_CANCEL_DT_SIZE
            + BN_RENEW_DT_SIZE
            + BN_STATE_NUM_SIZE;

    public static final int BN_ABN_OFFSET = RID_SIZE
            + REGISTER_NAME_SIZE
            + BN_NAME_SIZE
            + BN_STATUS_SIZE
            + BN_REG_DT_SIZE
            + BN_CANCEL_DT_SIZE
            + BN_RENEW_DT_SIZE
            + BN_STATE_NUM_SIZE
            + BN_STATE_OF_REG_SIZE;

    public void readArguments(String args[]);

    public boolean isInteger(String s);


    public static enum Column {
        BN_NAME("bn_name", BN_NAME_OFFSET, BN_NAME_SIZE),
        BN_STATUS("bn_status", BN_STATUS_OFFSET, BN_STATUS_SIZE),
        BN_REG_DT("bn_reg_dt", BN_REG_DT_OFFSET, BN_REG_DT_SIZE),
        BN_CANCEL_DT("bn_cancel_dt", BN_CANCEL_DT_OFFSET, BN_CANCEL_DT_SIZE),
        BN_RENEW_DT("bn_renew_dt", BN_RENEW_DT_OFFSET, BN_RENEW_DT_SIZE),
        BN_STATE_NUM("bn_state_num", BN_STATE_NUM_OFFSET, BN_STATE_NUM_SIZE),
        BN_STATE_OF_REG("bn_state_of_reg", BN_STATE_OF_REG_OFFSET, BN_STATE_OF_REG_SIZE),
        BN_ABN("bn_abn", BN_ABN_OFFSET, BN_ABN_SIZE);

        private String name;
        private int offset;
        private int length;


        Column(String name, int offset, int length) {
            this.name = name;
            this.offset = offset;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public int getOffset() {
            return offset;
        }

        public int getLength() {
            return length;
        }

        public static Column getColumnInfo(String c) {
            if (null == c || c.trim().equals("")) {
                return null;
            }

            c = c.trim().toLowerCase();
            Column[] columns = Column.values();
            for (Column column : columns) {
                if (column.name.equals(c)) {
                    return column;
                }
            }

            return null;
        }


    }

}
