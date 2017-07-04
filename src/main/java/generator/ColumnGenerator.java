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
  public int length = 0;
  public boolean numberString = false;

  private double nullProportion = 0.0;
  private double distinctProportion = 0.0;
  private HashMap<String,Integer> repeateMap;
  private ArrayList<String> repeateList;
  private int listSize = 1000;
  private Random rd;

  public ColumnGenerator() throws Exception {
    repeateMap = new HashMap<String, Integer>();
    repeateList = new ArrayList<String>();
    rd = new Random();
  }

  public ColumnGenerator(ColumnDefinition colDescriptor) {
    this.colDesc = colDescriptor;
    repeateMap = new HashMap<String, Integer>();
    repeateList = new ArrayList<String>();
    rd = new Random();
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
      distinctProportion = proportion;
    } else {
      throw new InvalidParameterException("Invalid distinct proportion");
    }
  }

  public void setListSize(int listSize) {
    this.listSize = listSize;
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

    if (Double.compare(distinctProportion, 0.0) > 0) {
      if (Double.compare(Math.random(), distinctProportion) < 0) {
        String str;
        if (repeateList.size() < listSize){
          str = getRandomValue();
          //calculate the amount of repeated words by random normal distribution stored into HashMap
          repeateMap.put(str,new Double(Math.abs(rd.nextGaussian()*50)).intValue());
          repeateList.add(str);
          return str;
        } else {
          int i = rd.nextInt(listSize);
          str = repeateList.get(i);
          if(repeateMap.get(str) == null) {
            repeateList.remove(i);
          } else if (repeateMap.get(str) == 0) {
            repeateMap.remove(str);
            repeateList.remove(i);
          } else {
            repeateMap.replace(str, repeateMap.get(str) - 1);
          }
          return str;
        }
      }
    }

    return  getRandomValue();
  }

  public String getRandomValue(){
    String type = colDesc.getColDataType().getDataType().toLowerCase();
    if (type.equals("int") || type.equals("long")) {
      return random.nextLong();
    } else if (type.equals("double") || type.equals("float")) {
      return random.nextDouble();
    } else if (type.startsWith("decimal")) {
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
    } else if (type.equals("string")) {
      if (numberString == true) {
        return random.nextNumber(length, true);
      }
      return random.nextString();
    } else if (type.equals("timestamp")) {
      return random.nextTimestamp();
    }
    return "";
  }
  
}
