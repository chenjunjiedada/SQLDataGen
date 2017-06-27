package generator;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import random.DataRandom;

import java.io.FileInputStream;
import java.security.InvalidParameterException;
import java.sql.Timestamp;
import java.util.*;

public class ColumnGenerator {
  public final int defaultScale = 10;
  public final int defaultPrecision = 6;

  public DataRandom random = new DataRandom();
  public ColumnDefinition colDesc;

  public double nullProportion = 0.0;
  //public double distinctProportion = 1;
    public Random rd = new Random();

  public ColumnGenerator() throws Exception {

  }

  public ColumnGenerator(ColumnDefinition colDescriptor) {
    this.colDesc = colDescriptor;
  }

  public void setColDesc(ColumnDefinition cd) {
    this.colDesc = cd;
  }

  public void setNullProportion(double proportion) throws InvalidParameterException {
    if (proportion > 0.0 && proportion < 1.0) {
      this.nullProportion = proportion;
    } else {
      throw new InvalidParameterException("Invalid null proportion");
    }
  }

  public void setDistinctProportion(double proportion) throws InvalidParameterException {
    if (proportion <= 1.0 || proportion > 0) {
      nullProportion = proportion;
    } else {
      throw new InvalidParameterException("Invalid distinct proportion");
    }
  }

  /*
      Generate random value in string according to column type.
      It also has to consider the null proportion.
   */
  public String nextValue() {
    if (Double.compare(nullProportion, 0.0) > 0) {
      if (Double.compare(Math.random(), nullProportion) < 0) {
        return "";
      }
    }

    /*
    if (Math.random() > distinctProportion) {
      // retrieve value from sampling data set;
    }
    */

    String type = colDesc.getColDataType().getDataType();
    if (type.toLowerCase().equals("int") || type.toLowerCase().equals("long")) {
//      return random.nextLong();
        return String.valueOf(rd.nextLong());
    } else if (type.toLowerCase().equals("double") || type.toLowerCase().equals("float")) {
//      return random.nextDouble();
        return String.valueOf(rd.nextDouble());
    } else if (type.toLowerCase().startsWith("decimal")) {
      List<String> params = colDesc.getColDataType().getArgumentsStringList();
      Integer scale = Integer.parseInt(params.get(0));
      Integer precision = Integer.parseInt(params.get(1));
      if (scale == null) {
        scale = defaultScale;
      }
      if (precision == null) {
        precision = defaultPrecision;
      }
//      return random.nextDecimal(scale, precision);
      if (precision==0){
          return rdNumber(scale-precision,true);
      }else
          return rdNumber(scale-precision,true)+"."+rdNumber(precision,false);
    } else if (type.toLowerCase().equals("string")) {
//      return random.nextString();
      return rdString(rd.nextInt(8)+8);
    } else if (type.toLowerCase().equals("timestamp")) {
      return random.nextTimestamp();
//      return (new Timestamp(new Date(117,5,11).getTime())).toString();
    }

    return "";

  }

  public String rdString(int length){
      String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//      Random random = new Random();
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < length; i++) {
          int num = rd.nextInt(62);
          buf.append(str.charAt(num));
      }
      return buf.toString();
  }

  public String rdNumber(int length,boolean rmfirstzero){
      if (length>0){
          String str = "0123456789";
          StringBuffer buf = new StringBuffer();
          for (int i = 0; i < length; i++) {
              int num = rd.nextInt(10);
              buf.append(str.charAt(num));
          }
          if (rmfirstzero){
              while (buf.length()>0&&buf.charAt(0)=='0'){
                  buf.deleteCharAt(0);
              }
          }
          if (buf.length()>0){
              return buf.toString();
          }else
              return "0";
      }else
          return "0";
  }
}
