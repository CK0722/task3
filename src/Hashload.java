import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Database Systems - HASH IMPLEMENTATION
 */
public class Hashload implements dbimpl {

    private static final int DEFAULT_SAME_RECORD_SIZE = 10;
    private String key;
    private int tableSize = 0x1FFF;         //8192
    private int bucketSize = 16;
    private List<List<IndexInfo>> indexTable;

    public Hashload(String key, int tableSize, int bucketSize) {
        this.key = key;
        this.tableSize = tableSize;
        this.bucketSize = bucketSize;
        this.indexTable = new ArrayList<>(tableSize);
        for (int i = 0; i < tableSize; ++i) {
            indexTable.add(new ArrayList<>(bucketSize));
        }
    }


    public static void main(String[] args) {
        Hashload hashload = new Hashload(Column.BN_NAME.getName(), 8192, 16);
        long startTime = System.currentTimeMillis();
        hashload.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Hash time: " + (endTime - startTime) + "ms");
    }


    @Override
    public void readArguments(String[] args) {
        if (args.length == 1) {
            if (isInteger(args[0])) {
                createIndex(Integer.parseInt(args[0]));
            }
        } else {
            System.out.println("Error: only pass in one argument");
        }
    }


    private void createIndex(int pageSize) {
        File heapfile = new File(HEAP_FNAME + pageSize);
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
                byte[] bPage = new byte[pageSize];
                byte[] bPageNum = new byte[intSize];
                fis.read(bPage, 0, pageSize);
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
                            generateIndex(bRecord, pageCount + 1, recordLen, pageSize);
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

            saveIndex(pageSize);
        } catch (FileNotFoundException e) {
            System.out.println("File: " + HEAP_FNAME + pageSize + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * store the index file
     *
     * @param pageSize
     */
    private void saveIndex(int pageSize) {
        long start = System.currentTimeMillis();
        try {
            BufferedWriter fw = new BufferedWriter(new FileWriter(INDEX_FNAME + pageSize));
            for (List<IndexInfo> infoList : indexTable) {
                if (!infoList.isEmpty()) {
                    for (IndexInfo indexInfo : infoList) {
                        fw.write(indexInfo.toString());
                        fw.write(" ");
                    }
                }
                fw.newLine();
            }
            fw.flush();
            fw.close();
            long end = System.currentTimeMillis();
            System.out.println("Save index file costs: " + (end - start) + "ms");
        } catch (FileNotFoundException e) {
            System.out.println("File: " + INDEX_FNAME + pageSize + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private void generateIndex(byte[] record, int pageNum, int rowNum, int pageSize) {
        String colValue = getColValue(record, key);
        if (null == colValue || colValue.trim().equals("")) {
            return;
        }
        colValue=colValue.trim().toLowerCase();

        int hashIndex = indexFor(colValue);
        int realPosition = pageNum * pageSize + rowNum * RECORD_SIZE;

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


    private String getColValue(byte[] record, String key) {
        Column columnInfo = Column.getColumnInfo(key);
        String entry = new String(record);
        String colValue = entry.substring(columnInfo.getOffset(), columnInfo.getOffset() + columnInfo.getLength());
        return colValue;
    }

    private int indexFor(String value) {
        int hashCode = value.trim().toLowerCase().hashCode();
        int index = Math.abs(hashCode) % tableSize;
        return index;
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
}
