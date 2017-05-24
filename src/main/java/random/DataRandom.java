package random;

import org.apache.commons.lang.RandomStringUtils;
import org.w3c.dom.ranges.RangeException;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;


/*
 DataRandom is use to generate string according data type.

 Load all dicts to memory for random read.
 */
public class DataRandom {

    private HashMap<String, ArrayList<String>> dictMap = new HashMap<String, ArrayList<String>>();

    public DataRandom () {
    }

    public void initDicts () throws IOException{
        final String dir = System.getProperty("user.dir");

        try {
            File[] files = new File(dir + "/dicts/").listFiles();

            for (File file : files) {
                if (file.isFile() && file.getName().endsWith("dict")) {
                    BufferedReader br = new BufferedReader(new FileReader(file.getName()));
                    ArrayList<String> dict = new ArrayList<String>();
                    String line;
                    while ((line = br.readLine()) != null) {
                        dict.add(line);
                    }
                    dictMap.put(file.getName(), dict);
                }
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public String nextLong(int scale) {
        return RandomStringUtils.random(scale, false, true);
    }

    public String nextDecimal(int scale, int precision) {
        String part1 = nextLong(scale-precision);
        String part2 = nextLong(precision);
        return part1 + "." + part2;
    }

    public String nextTimestamp() {
        long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));
        return rand.toString();
    }

    public String nextName() {
        ArrayList<String> dict = dictMap.get("Given-Names.dict");
        int count = dict.size();

        Random r = new Random();
        return dict.get(r.nextInt(count));
    }

    public String nextString() {
        Random r = new Random();
        return RandomStringUtils.randomAlphabetic(r.nextInt(20));
    }


}
