import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * query data with index
 */
public class Hashquery implements dbimpl {

    public static void main(String[] args) {
        Hashquery hashquery = new Hashquery();
        try {
            hashquery.loadIndexFile(4096);
        } catch (IOException e) {
            e.printStackTrace();
        }


//        long startTime = System.currentTimeMillis();
//        hashquery.readArguments(args);
//        long endTime = System.currentTimeMillis();
//
//        System.out.println("Query time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void readArguments(String[] args) {
        if (args.length == 2) {
            if (isNotEmpty(args[0]) && isInteger(args[1])) {
                queryText(args[0],Integer.parseInt(args[1]));
            }
        } else {
            System.out.println("Error: only pass in two argument");
        }
    }


    private void queryText(String text, int pageSize) {

    }

    @Override
    public boolean isInteger(String s) {
        return false;
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

}
