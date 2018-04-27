/**
 * query data with index
 */
public class Hashquery implements dbimpl {

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

            }
        } else {
            System.out.println("Error: only pass in two argument");
        }
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

}
