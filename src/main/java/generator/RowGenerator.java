package generator;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowGenerator {
  private long expectedRows = 1;
  private List<ColumnGenerator> cgs = new ArrayList<ColumnGenerator>();
  private CreateTable createTableStat;
  private String partitionInfo;
  ColumnGenerator partitionGenerator = new ColumnGenerator();

  public RowGenerator(String sql) throws Exception {

    String createTableSql = sql;

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

    for (ColumnDefinition columnDefinition : createTableStat.getColumnDefinitions()) {
      ColumnGenerator cg = new ColumnGenerator(columnDefinition);
      cgs.add(cg);
    }

    // List<String> options = createTableStat.
  }

  public void setExpectedRows(long expectedRows) {
    this.expectedRows = expectedRows;
  }


  //how to estimate?
  public int getBytesInRow() {
    return cgs.size() * 16;
  }

  public int generateData(String uri) throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://bdpe611n1:9001");
    FileSystem hdfs = FileSystem.get(conf);
    Path file = new Path(uri);

    if (hdfs.exists(file)) {
      hdfs.delete(file, true);
    }

    OutputStream os = hdfs.create(file);

    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));

    for (long i = 0; i < expectedRows; i++) {
      br.write(nextRow() + "\n");
    }
    br.flush();
    br.close();
    hdfs.close();
    return 0;
  }

  public String nextRow() {
    String output = "";
    for (ColumnGenerator cg : cgs) {
      output += cg.nextValue();
      output += "|";
    }
    return output;
  }
}
