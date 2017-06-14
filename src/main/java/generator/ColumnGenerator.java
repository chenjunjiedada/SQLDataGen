package generator;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import random.DataRandom;

import java.io.FileInputStream;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ColumnGenerator {
  public final int defaultScale = 10;
  public final int defaultPrecision = 6;

  public DataRandom random = new DataRandom();
  public ColumnDefinition colDesc;

  public double nullProportion = 0.0;
  //public double distinctProportion = 1;

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
    if (Math.random() < nullProportion) return "";

    /*
    if (Math.random() > distinctProportion) {
      // retrieve value from sampling data set;
    }
    */

    String type = colDesc.getColDataType().getDataType();
    if (type.toLowerCase().equals("int") || type.toLowerCase().equals("long")) {
      return random.nextLong();
    } else if (type.toLowerCase().equals("double") || type.toLowerCase().equals("float")) {
      return random.nextDouble();
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
      return random.nextDecimal(scale, precision);
    } else if (type.toLowerCase().equals("string")) {
      return random.nextString();
    } else if (type.toLowerCase().equals("timestamp")) {
      return random.nextTimestamp();
    }

    return "";

  }
}
