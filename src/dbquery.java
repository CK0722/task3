import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public class dbquery implements dbimpl {
    // initialize
    public static void main(String args[]) {
        dbquery load = new dbquery();

        // calculate query time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }


    /**
     * reading command line arguments
     *
     * @param args
     */
    @Override
    public void readArguments(String args[]) {
        if (args.length == 2) {
            if (isInteger(args[1])) {
                readHeap(args[0], Column.BN_ABN.getName(), Integer.parseInt(args[1]));
            }
        } else {
            System.out.println("Error: only pass in two arguments");
        }
    }

    /**
     * check if pagesize is a valid integer
     *
     * @param s
     * @return
     */
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


    /**
     * read heapfile by page
     *
     * @param text
     * @param key
     * @param pagesize
     */
    public void readHeap(String text, String key, int pagesize) {
        File heapfile = new File(HEAP_FNAME + pagesize);
        int intSize = 4;
        int pageCount = 0;
        int recCount = 0;
        int recordLen = 0;
        int rid = 0;
        boolean isNextPage = true;
        boolean isNextRecord = true;
        try {
            FileInputStream fis = new FileInputStream(heapfile);
            // reading page by page
            while (isNextPage) {
                byte[] bPage = new byte[pagesize];
                byte[] bPageNum = new byte[intSize];
                fis.read(bPage, 0, pagesize);
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
                            printRecord(bRecord, key, text);
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
        } catch (FileNotFoundException e) {
            System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * returns records containing the argument text from shell
     *
     * @param rec
     * @param key
     * @param text
     */
    public void printRecord(byte[] rec, String key, String text) {
        Column columnInfo = Column.getColumnInfo(key);
        String record = new String(rec);
        String colValue = record.substring(columnInfo.getOffset(), columnInfo.getOffset() + columnInfo.getLength());
        if (colValue.toLowerCase().contains(text.toLowerCase())) {
            System.out.println(record);
        }
    }
}
