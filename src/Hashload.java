import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Database Systems - HASH IMPLEMENTATION
 */
public class Hashload implements dbimpl {

    /**
     * The largest possible table capacity.  This value must be
     * exactly 1<<30 to stay within Java array allocation and indexing
     * bounds for power of two table sizes, and is further required
     * because the top two bits of 32bit hash fields are used for
     * control purposes.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;


    /**
     * the default value of the size of the positions of the same record
     */
    private static final int DEFAULT_SAME_RECORD_SIZE = 10;


    /**
     * the default size of the index table
     */
    private static final int DEFAULT_TABLE_SIZE = 8192;


    /**
     * the default size of each bucket of the index table
     */
    private static final int DEFAULT_BUCKET_SIZE = 400;

    /**
     * the column to be indexed
     * you can modify the value in the main function
     */
    private String key;

    /**
     * the maxinum bytes of each index page
     * tests verifies the value cann't be lower than 10240
     */
    private int pageSize;

    /**
     * the maxinum size of index table
     * it will impact hash collision costs
     * it will call <code>tableSizeFor</code> returns a power of two table size for your given desired capacity
     */
    private int tableSize;

    /**
     * the nitial capacity of each bucket of index table
     * tests verifies 400 is appropriate the data
     */
    private int bucketSize;

    /**
     * the bitwise mask
     * it equals the tableSize minus 1
     * it can improve the hash efficiency
     */
    private int modules;
    private List<List<IndexInfo>> indexTable;

    public Hashload(String key, int tableSize, int bucketSize) {
        this.key = key;
        this.tableSize = tableSize;
        this.bucketSize = bucketSize;
        init();
    }

    public Hashload(String key, int pageSize) {
        this.key = key;
        this.pageSize = pageSize;
        this.tableSize = DEFAULT_TABLE_SIZE;
        this.bucketSize = DEFAULT_BUCKET_SIZE;
        init();
    }

    public Hashload(int pageSize) {
        this.pageSize = pageSize;
        this.tableSize = DEFAULT_TABLE_SIZE;
        this.bucketSize = DEFAULT_BUCKET_SIZE;
        init();
    }

    public Hashload(String key) {
        this.key = key;
        this.tableSize = DEFAULT_TABLE_SIZE;
        this.bucketSize = DEFAULT_BUCKET_SIZE;
        init();
    }

    private void init() {
        this.tableSize = tableSizeFor(this.tableSize);
        this.modules = this.tableSize - 1;
        this.indexTable = new ArrayList<>(this.tableSize);
        for (int i = 0; i < tableSize; ++i) {
            this.indexTable.add(new ArrayList<>(this.bucketSize));
        }
    }

    public static void main(String[] args) {
        Hashload hashload = new Hashload(Column.BN_ABN.getName());
        long startTime = System.currentTimeMillis();
        hashload.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Hash time: " + (endTime - startTime) + "ms");
    }


    @Override
    public void readArguments(String[] args) {
        if (args.length == 1) {
            if (isInteger(args[0])) {
                this.pageSize = Integer.parseInt(args[0]);
                createIndex();
            }
        } else {
            System.out.println("Error: only pass in one argument");
        }
    }

    @Override
    public boolean isInteger(String s) {
        boolean isValidInt = false;
        try {
            Integer.parseInt(s);
            isValidInt = true;
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return isValidInt;
    }

    @Override
    public boolean isValidStr(String s) {
        if (null == s || s.trim().equals("")) {
            return false;
        }
        if ("\t".equals(s) || "\r".equals(s) || "\n".equals(s) || "\r\n".equals(s)) {
            return false;
        }

        return true;
    }


    /**
     * @param index the value of the posiotion of  index information(the bucket location)
     * @return index information
     * @throws IOException
     */
    public List<IndexInfo> loadIndex(int index) throws IOException {
        long start = System.currentTimeMillis();
        ArrayList<IndexInfo> indexInfos = new ArrayList<>(this.bucketSize);

        byte[] indexRecord = new byte[this.pageSize];
        FileInputStream fis = new FileInputStream(loadIndexFile());
        fis.skip(this.pageSize * index);
        fis.read(indexRecord, 0, this.pageSize);

        String indexStr = new String(indexRecord);
        String[] split = indexStr.split(INDEX_SEP);
        for (String str : split) {
            indexInfos.add(new IndexInfo(str));
        }

        long end = System.currentTimeMillis();
        System.out.println("Load index table costs: " + (end - start) + "ms");
        return indexInfos;
    }


    public File loadIndexFile() {
        File file = new File(INDEX_FNAME + this.pageSize);
        return file;
    }


    /**
     * record index information of each record in the heap file
     */
    private void createIndex() {
        dbload dbload = new dbload();
        File heapfile = dbload.loadHeapFile();
        int defaultHeapFileSize = dbload.getPageSize();
        int intSize = 4;
        int pageCount = 0;
        int recCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextPage = true;
        boolean isNextRecord = true;
        try {
            FileInputStream fis = new FileInputStream(heapfile);
            while (isNextPage) {
                byte[] bPage = new byte[defaultHeapFileSize];
                byte[] bPageNum = new byte[intSize];
                fis.read(bPage, 0, defaultHeapFileSize);
                System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);

                // reading by record, return true to read the next record
                isNextRecord = true;
                while (isNextRecord) {
                    byte[] bRecord = new byte[RECORD_SIZE];
                    byte[] bRid = new byte[intSize];
                    try {
                        System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
                        System.arraycopy(bRecord, 0, bRid, 0, intSize);
                        rid = ByteBuffer.wrap(bRid).getInt();
                        if (rid != recCount) {
                            isNextRecord = false;
                        } else {
                            generateIndex(bRecord, pageCount, recordLen, defaultHeapFileSize);
                            recordLen += RECORD_SIZE;
                        }
                        recCount++;
                        // if recordLen exceeds pagesize, catch this to reset to next page
                    } catch (ArrayIndexOutOfBoundsException e) {
                        isNextRecord = false;
                        recordLen = 0;
                        recCount = 0;
                        rid = 0;
                    }
                }
                // check to complete all pages
                if (ByteBuffer.wrap(bPageNum).getInt() != pageCount) {
                    isNextPage = false;
                }
                pageCount++;
            }

            storeIndex();
        } catch (FileNotFoundException e) {
            System.out.println("File: " + heapfile.getName() + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * store the index file
     */
    private void storeIndex() {
        long start = System.currentTimeMillis();
        File indexFile = loadIndexFile();
        try {
            FileOutputStream fout = new FileOutputStream(indexFile);
            int occupy = 0;
            for (List<IndexInfo> infoList : indexTable) {
                byte[] bPage = new byte[this.pageSize];
                int totalBytes = 0;
                if (!infoList.isEmpty()) {
                    for (IndexInfo indexInfo : infoList) {
                        String indexs = indexInfo.toString() + INDEX_SEP;
                        byte[] dataSrc = indexs.getBytes(ENCODING);
                        System.arraycopy(dataSrc, 0, bPage, totalBytes, dataSrc.length);
                        totalBytes += dataSrc.length;
                    }
                    ++occupy;
                }
                fout.write(bPage);
            }
            fout.flush();
            fout.close();
            System.out.println("Page total: " + indexTable.size());
            System.out.println("Occupy total: " + occupy);
            long end = System.currentTimeMillis();
            System.out.println("Save index file costs: " + (end - start) + "ms");
        } catch (FileNotFoundException e) {
            System.out.println("File: " + indexFile.getName() + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * @param record              data
     * @param pageNum             the page value of the record stored in the heap file
     * @param rowOffset           the offset of the record in the page
     * @param defaultHeapFileSize the maxinum of bytes in each page of the heap file
     */
    private void generateIndex(byte[] record, int pageNum, int rowOffset, int defaultHeapFileSize) {
        String colValue = getColValue(record, key);
        if (!isValidStr(colValue)) {
            return;
        }
        colValue = colValue.trim().toLowerCase();

        int hashIndex = indexFor(colValue);
        int realPosition = pageNum * defaultHeapFileSize + rowOffset;

        //to handle collisions
        boolean hasIndex = false;
        List<IndexInfo> indexInfos = indexTable.get(hashIndex);
        for (IndexInfo info : indexInfos) {
            if (colValue.equals(info.getValue())) {
                info.addPosition(realPosition);
                hasIndex = true;
                break;
            }
        }

        if (!hasIndex) {
            IndexInfo indexInfo = new IndexInfo(DEFAULT_SAME_RECORD_SIZE, colValue, realPosition);
            indexInfos.add(indexInfo);
        }

    }

    /**
     * @param record data
     * @param key    the column name of the dataset which is to be indexed
     * @return the column value of the data
     */
    private String getColValue(byte[] record, String key) {
        Column columnInfo = Column.getColumnInfo(key);
        String entry = new String(record);
        String colValue = entry.substring(columnInfo.getOffset(), columnInfo.getOffset() + columnInfo.getLength());
        return colValue;
    }


    /**
     * @param value the column value recorded in the heap file
     * @return the index value of the specified text
     */
    public int indexFor(String value) {
        int h = value.trim().toLowerCase().hashCode();
        h = h ^ (h >>> 16);
        h = Math.abs(h);

        int index = h & this.modules;
        return index;
    }


    /**
     * Returns a power of two table size for the given desired capacity.
     * See Hackers Delight, sec 3.2
     */
    private static final int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }


    /**
     * store the index file
     */
    @Deprecated
    private void saveIndex() {
        long start = System.currentTimeMillis();
        File indexFile = loadIndexFile();
        try {
            BufferedWriter fw = new BufferedWriter(new FileWriter(indexFile));
            for (List<IndexInfo> infoList : indexTable) {
                if (!infoList.isEmpty()) {
                    for (IndexInfo indexInfo : infoList) {
                        fw.write(indexInfo.toString());
                        fw.write(INDEX_SEP);
                    }
                }
                fw.newLine();
            }
            fw.flush();
            fw.close();
            long end = System.currentTimeMillis();
            System.out.println("Save index file costs: " + (end - start) + "ms");
        } catch (FileNotFoundException e) {
            System.out.println("File: " + indexFile.getName() + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
