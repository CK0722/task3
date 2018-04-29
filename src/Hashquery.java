import java.io.*;
import java.util.List;

/**
 * query data with index
 */
public class Hashquery implements dbimpl {

    public static void main(String[] args) {
        Hashquery hashquery = new Hashquery();
//        try {
//            hashquery.loadIndexFile(4096);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        long startTime = System.currentTimeMillis();
        hashquery.readArguments(args);
        long endTime = System.currentTimeMillis();

        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void readArguments(String[] args) {
        if (args.length == 2) {
            if (isNotEmpty(args[0]) && isInteger(args[1])) {
                queryText(args[0], Integer.parseInt(args[1]), 8192, 400, Column.BN_ABN.getName());
            }
        } else {
            System.out.println("Error: only pass in two argument");
        }
    }


    private void queryText(String text, int pageSize, int tableSize, int bucketSize, String key) {
        try {
            //calculate the index value based on the text
            Hashload hashload = new Hashload(key, tableSize, bucketSize);
            int index = hashload.indexFor(text);

            //locate the record position  in the heap file based on the index value
            List<IndexInfo> indexEntries = hashload.loadIndexInfo(pageSize, index);
            List<Integer> positions = null;
            for (IndexInfo entry : indexEntries) {
                //has found the record positions
                if (text.equalsIgnoreCase(entry.getValue())) {
                    positions = entry.getPositions();
                    break;
                }
            }

            //no record
            if (null == positions || positions.isEmpty()) {
                return;
            }
//            Integer initialPos = positions.get(0);
//            for (int i = 1; i < positions.size(); i++) {
//                positions.set(i, positions.get(i) - initialPos);
//            }

            RandomAccessFile reader = new RandomAccessFile(HEAP_FNAME + pageSize, "r");
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

    public boolean isNotEmpty(String s) {
        if (null == s || s.trim().equals("")) {
            return false;
        }
        return true;
    }

    public void loadIndexFile(int pageSize) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(INDEX_FNAME + pageSize));
        String line;
        while (null != (line = br.readLine())) {
            System.out.println(line);
        }
    }

    public void printRecord(byte[] rec) {
        String record = new String(rec);
        System.out.println(record);
    }

}
