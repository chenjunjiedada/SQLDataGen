package random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.sql.Timestamp;
import java.util.Random;

/*
 DataRandom is use to generate string according data type.

 Load all dicts to memory for random read.
 */
public class DataRandom {
  private Random r = new Random();
  public final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  public final String number = "0123456789";

  public DataRandom() {
  }


  public String nextLong(int scale) {
    return RandomStringUtils.random(scale, false, true);
  }

  public String nextLong() {
    return Long.toString(r.nextLong());
  }

  public String nextFloat() {
    return Float.toString(RandomUtils.nextFloat());
  }

  public String nextDouble() {
    return Double.toString(RandomUtils.nextDouble());
  }

  public String nextNumber(int length, boolean not_start_with_zero) {
    if (length <= 0) return "";

    StringBuffer buf = new StringBuffer();

    if (not_start_with_zero) {
      buf.append(number.charAt(r.nextInt(8) + 1));
      length --;
      nextNumber(length, false);
    }

    for (int i=0; i<length; i++) {
      buf.append(number.charAt(r.nextInt(9)));
    }

    return buf.toString();
  }

  public String nextDecimal(int scale, int precision) {
    if (precision == 0) {
      return nextNumber(scale - precision, true);
    } else {
      return nextNumber(scale - precision, true) + nextNumber(precision, true);
    }
  }

  public String nextTimestamp() {
    long start = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
    long end = Timestamp.valueOf("2017-01-01 00:00:00").getTime();
    long diff = end - start + 1;
    Timestamp rand = new Timestamp(start + Math.abs(r.nextLong() % diff));
    return rand.toString();
  }

  public String nextString(int length) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < length; i++) {
      int num = r.nextInt(alphabet.length());
      buf.append(alphabet.charAt(num));
    }
    return buf.toString();
  }

  public String nextString() {
    return nextString(r.nextInt(20) + 4);
  }
}
