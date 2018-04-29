import java.io.*;
import java.nio.ByteBuffer;

/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public class dbload implements dbimpl {

    private static final int DEFAULT_HEAP_PAGE_SIZE = 4096;
    private int pageSize;

    public int getPageSize() {
        return pageSize;
    }

    public dbload() {
        this.pageSize = DEFAULT_HEAP_PAGE_SIZE;
    }

    public dbload(int pageSize) {
        this.pageSize = pageSize;
    }

    // initialize
    public static void main(String args[]) {
        dbload load = new dbload();

        // calculate load time
        long startTime = System.currentTimeMillis();
        load.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Load time: " + (endTime - startTime) + "ms");
    }

    /**
     * reading command line arguments
     *
     * @param args
     */
    @Override
    public void readArguments(String args[]) {
        if (args.length == 3) {
            if (args[0].equals("-p") && isInteger(args[1])) {
                this.pageSize = Integer.parseInt(args[1]);
                readFile(args[2]);
            }
        } else {
            System.out.println("Error: only pass in three arguments");
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

    @Override
    public boolean isValidStr(String str) {
        return false;
    }


    /**
     * read .csv file using buffered reader
     *
     * @param fileName
     */
    public void readFile(String fileName) {
        File heapfile = loadHeapFile();
        BufferedReader br = null;
        FileOutputStream fos = null;
        String line = "";
        String stringDelimeter = "\t";
        byte[] RECORD = new byte[RECORD_SIZE];
        int outCount, pageCount, recCount;
        outCount = pageCount = recCount = 0;

        try {
            // create stream to write bytes to according page size
            fos = new FileOutputStream(heapfile);
            br = new BufferedReader(new FileReader(fileName));
            // read line by line
            while ((line = br.readLine()) != null) {
                String[] entry = line.split(stringDelimeter, -1);
                RECORD = createRecord(RECORD, entry, outCount);
                // outCount is to count record and reset everytime
                // the number of bytes has exceed the pagesize
                outCount++;
                fos.write(RECORD);
                if ((outCount + 1) * RECORD_SIZE > pageSize) {
                    eofByteAddOn(fos, pageSize, outCount, pageCount);
                    //reset counter to start newpage
                    outCount = 0;
                    pageCount++;
                }
                recCount++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("File: " + fileName + " not found.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    // final add on at end of file
                    if ((br.readLine()) == null) {
                        eofByteAddOn(fos, pageSize, outCount, pageCount);
                        pageCount++;
                    }
                    fos.close();
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Page total: " + pageCount);
        System.out.println("Record total: " + recCount);
    }

    public File loadHeapFile() {
        return new File(HEAP_FNAME + this.pageSize);
    }


    /**
     * create byte array for a field and append to record array at correct
     * offset using array copy
     *
     * @param entry
     * @param SIZE
     * @param DATA_OFFSET
     * @param rec
     * @throws UnsupportedEncodingException
     */
    public void copy(String entry, int SIZE, int DATA_OFFSET, byte[] rec)
            throws UnsupportedEncodingException {
        byte[] DATA = new byte[SIZE];
        byte[] DATA_SRC = entry.trim().getBytes(ENCODING);
        if (entry != "") {
            System.arraycopy(DATA_SRC, 0,
                    DATA, 0, DATA_SRC.length);
        }
        System.arraycopy(DATA, 0, rec, DATA_OFFSET, DATA.length);
    }


    /**
     * creates record by appending using array copy and then applying offset
     * where neccessary
     *
     * @param rec
     * @param entry
     * @param out
     * @return
     * @throws UnsupportedEncodingException
     */
    public byte[] createRecord(byte[] rec, String[] entry, int out)
            throws UnsupportedEncodingException {
        byte[] RID = intToByteArray(out);
        System.arraycopy(RID, 0, rec, 0, RID.length);

        copy(entry[0], REGISTER_NAME_SIZE, RID_SIZE, rec);

        copy(entry[1], BN_NAME_SIZE, BN_NAME_OFFSET, rec);

        copy(entry[2], BN_STATUS_SIZE, BN_STATUS_OFFSET, rec);

        copy(entry[3], BN_REG_DT_SIZE, BN_REG_DT_OFFSET, rec);

        copy(entry[4], BN_CANCEL_DT_SIZE, BN_CANCEL_DT_OFFSET, rec);

        copy(entry[5], BN_RENEW_DT_SIZE, BN_RENEW_DT_OFFSET, rec);

        copy(entry[6], BN_STATE_NUM_SIZE, BN_STATE_NUM_OFFSET, rec);

        copy(entry[7], BN_STATE_OF_REG_SIZE, BN_STATE_OF_REG_OFFSET, rec);

        copy(entry[8], BN_ABN_SIZE, BN_ABN_OFFSET, rec);

        return rec;
    }


    /**
     * EOF padding to fill up remaining pagesize
     * minus 4 bytes to add page number at end of file
     *
     * @param fos
     * @param pSize
     * @param out
     * @param pCount
     * @throws IOException
     */
    public void eofByteAddOn(FileOutputStream fos, int pSize, int out, int pCount)
            throws IOException {
        byte[] fPadding = new byte[pSize - (RECORD_SIZE * out) - 4];
        byte[] bPageNum = intToByteArray(pCount);
        fos.write(fPadding);
        fos.write(bPageNum);
    }


    /**
     * converts ints to a byte array of allocated size using bytebuffer
     *
     * @param i
     * @return
     */
    public byte[] intToByteArray(int i) {
        ByteBuffer bBuffer = ByteBuffer.allocate(4);
        bBuffer.putInt(i);
        return bBuffer.array();
    }
}
