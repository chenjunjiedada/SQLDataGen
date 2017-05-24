package random;

import org.w3c.dom.ranges.RangeException;

public class DataRandom {

    public static String nextLong(int scale) throws RangeException {
        java.util.Random rand = new java.util.Random();
        if (scale > 20) {
            throw new RangeException(RangeException.BAD_BOUNDARYPOINTS_ERR, "scale too large");
        }
        String output = "";
        for (int i = 0; i < scale; i++) {
            output += Integer.toString(rand.nextInt(10));
        }
        return output;
    }


    public static String nextDecimal(int scale, int precision) {
        String part1 = nextLong(scale-precision);
        String part2 = nextLong(precision);
        return part1 + "." + part2;
    }

    public static String nextTimestamp() {
        return "";
    }

    public static String nextString() {
        return "";
    }


}
