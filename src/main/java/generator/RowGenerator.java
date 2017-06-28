package generator;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.calcite.avatica.proto.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class RowGenerator extends Thread {
  private long expectedRows = 1;
  private List<ColumnGenerator> cgs = new ArrayList<ColumnGenerator>();
  private CreateTable createTableStat;
  private String partitionInfo;
  private Properties columnProperties = new Properties();
  private double distinctProportion = 0.2;
  private HashMap<String,Integer> repeateMap;
  private ArrayList<String> repeateList;
  private Random rd = new Random();

  ColumnGenerator partitionGenerator = new ColumnGenerator();
  private Properties props = new Properties();
  public String targetPath;
  public String filesystemHost;
  public String createTableSql;
//  public double distinctProportion = 0.05;


  public RowGenerator(String sql, String path) throws Exception {
    this(sql);
    this.targetPath = path;
  }

  public RowGenerator(String sql) throws Exception {
    repeateMap = new HashMap<String, Integer>();
    repeateList = new ArrayList<String>();
    createTableSql = sql;

    if (sql.toLowerCase().indexOf("partitioned by") != -1) {
      createTableSql = sql.substring(0, sql.toLowerCase().indexOf("partitioned by"));
      partitionInfo = sql.substring(createTableSql.length());

      String partitionColumn = partitionInfo.substring(partitionInfo.indexOf('(')+1, partitionInfo.lastIndexOf(')'));
      String partitionColumnName = partitionColumn.substring(0, partitionColumn.trim().indexOf(' '));
      String partitionColumnType = partitionColumn.substring(partitionColumn.trim().indexOf(' '));

      ColumnDefinition cd = new ColumnDefinition();
      ColDataType columnType = new ColDataType();

      if (partitionColumnType.indexOf('(') != -1) {
        String arguments = partitionColumnType.substring(partitionColumnType.indexOf('(') + 1, partitionColumnType.lastIndexOf(')'));
        columnType.setArgumentsStringList(Arrays.asList(arguments.split(",")));
        partitionColumnType = partitionColumnType.substring(0, partitionColumnType.indexOf('('));
      }
      columnType.setDataType(partitionColumnType);

      cd.setColDataType(columnType);

      cd.setColumnName(partitionColumnName);
      partitionGenerator.setColDesc(cd);
    }

    Statement st = CCJSqlParserUtil.parse(createTableSql);
    if (!(st instanceof CreateTable)) {
      throw new RuntimeException("Not a valid create table SQL");
    }
    this.createTableStat = (CreateTable) st;

    getColumnProperties();

    distinctProportion = Double.valueOf(columnProperties.getProperty("user_num.distinct.proportion"));
    System.out.println(distinctProportion);
    for (ColumnDefinition columnDefinition : createTableStat.getColumnDefinitions()) {
      ColumnGenerator cg = new ColumnGenerator(columnDefinition);
      String columnName = columnDefinition.getColumnName();
      String value;
      if ((value = columnProperties.getProperty(columnName + ".null.proportion")) != null) {
        cg.setNullProportion(Double.parseDouble(value));
      }

      if ((value = columnProperties.getProperty(columnName + ".distinct.proportion")) != null) {
        cg.setDistinctProportion(Double.parseDouble(value));
      }
      cgs.add(cg);
    }

  }

  public void setExpectedRows(long expectedRows) {
    this.expectedRows = expectedRows;
  }
  public long getExpectedRows() {return expectedRows;}

  public void setTargetPath(String targetPath) {
    this.targetPath = targetPath;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public void setFilesystemHost(String host) {
    this.filesystemHost = host;
  }

  public int getBytesInRow() {
    return cgs.size() * 16;
  }

  @Override
  public void run() {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", filesystemHost);

    try {
      FileSystem hdfs = FileSystem.get(conf);
      Path file = new Path(targetPath);

      if (hdfs.exists(file)) {
        hdfs.delete(file, true);
      }

      OutputStream os = hdfs.create(file);
      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os), 32768);

      for (long i = 0; i < expectedRows; i++) {
        br.write(nextRow() + "\n");
      }
      br.close();
      hdfs.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void produceRow() throws Exception {
    start();
  }

  public String nextRow() {
    String output = "";
    for (ColumnGenerator cg : cgs) {
      if (cg.colDesc.getColumnName()=="user_num"){
        output += repeatebysettings(cg);
        output += "|";
      }else{
        output += cg.nextValue();
        output += "|";
      }
    }
    return output;
  }


  public void getColumnProperties() throws IOException {
    String project_root = System.getProperty("user.dir");
    FileInputStream fis = new FileInputStream(project_root + "/engines/hive/conf/columns.properties");
    columnProperties.load(fis);
  }

  public String repeatebysettings(ColumnGenerator cg) {
    String str;
    if (Math.random() > distinctProportion){
      if (repeateList.size() < 1000){
        str = cg.nextValue();
        repeateMap.put(str,new Double(Math.abs(rd.nextGaussian()*50)).intValue());
        repeateList.add(str);
        return str;
      }else{
        int i = rd.nextInt(1000);
        str = repeateList.get(i);
        if (repeateMap.get(str)==0){
          repeateMap.remove(str);
          repeateList.remove(i);
        }else{
          repeateMap.put(str,repeateMap.get(str)-1);
        }
        return str;
      }
    }else{
      return cg.nextValue();
    }
  }

}
