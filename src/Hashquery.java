import java.io.*;
import java.util.List;

/**
 * query data with index
 */
public class Hashquery implements dbimpl {

    private int pageSize;
    private String queryText;

    public Hashquery() {
    }

    public Hashquery(int pageSize, String queryText) {
        this.queryText = queryText;
        this.pageSize = pageSize;
    }

    public static void main(String[] args) {
        Hashquery hashquery = new Hashquery();
        long startTime = System.currentTimeMillis();
        hashquery.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void readArguments(String[] args) {
        if (args.length == 2) {
            if (isNotEmpty(args[0]) && isInteger(args[1])) {
                this.queryText = args[0];
                this.pageSize = Integer.parseInt(args[1]);
                this.queryText();
            }
        } else {
            System.out.println("Error: only pass in two argument");
        }
    }


    private void queryText() {
        try {
            //calculate the index value based on the text
            Hashload hashload = new Hashload(this.pageSize);
            int index = hashload.indexFor(this.queryText);

            //locate the record position  in the heap file based on the index value
            List<IndexInfo> indexEntries = hashload.loadIndex(index);
            List<Integer> positions = null;
            for (IndexInfo entry : indexEntries) {
                //has found the record positions
                if (this.queryText.equalsIgnoreCase(entry.getValue())) {
                    positions = entry.getPositions();
                    break;
                }
            }

            //no record
            if (null == positions || positions.isEmpty()) {
                return;
            }

            dbload dbload = new dbload();
            RandomAccessFile reader = new RandomAccessFile(dbload.loadHeapFile(), "r");
            byte[] record = new byte[RECORD_SIZE];
            positions.forEach(pos -> {
                try {
                    reader.seek(pos);
                    reader.read(record, 0, RECORD_SIZE);
                    printRecord(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
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
    public boolean isValidStr(String str) {
        return false;
    }


    public void printRecord(byte[] rec) {
        String record = new String(rec);
        System.out.println(record);
    }


    private boolean isNotEmpty(String s) {
        if (null == s || s.trim().equals("")) {
            return false;
        }
        return true;
    }

    private void printIndexFile(int pageSize) throws IOException {
        Hashload hashload = new Hashload(pageSize);
        File indexFile = hashload.loadIndexFile();
        BufferedReader br = new BufferedReader(new FileReader(indexFile));
        String line;
        while (null != (line = br.readLine())) {
            System.out.println(line);
        }
    }


}
