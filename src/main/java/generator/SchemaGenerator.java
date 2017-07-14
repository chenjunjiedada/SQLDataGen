package generator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SchemaGenerator {
  private final long MB = 1024 * 1024;
  private List<RowGenerator> rgs = new ArrayList<RowGenerator>();
  public int scale = 1;
  private String host;
  private String storePath;

  public void setScale(int scale) {
    this.scale = scale;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setStorePath(String path) {
    this.storePath = path;
  }

  static public int id = 0;

  public List<String> parseCreateTable(String sqlFile) throws Exception {
    FileReader fileReader = new FileReader(sqlFile);
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    List<String> sqls = new ArrayList<String>();

    String query = "";
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      query = query + line + "\n";
      if (line.contains(";")) {
        if (query.toLowerCase().contains("create external")) {
          sqls.add(query.substring(0, query.lastIndexOf(')') + 1) + ";");
        }
        query = "";
      }
    }
    return sqls;
  }

  public void addRowGenerator(String createTable) throws Exception {
    RowGenerator rg = new RowGenerator(createTable);
    rg.setExpectedRows(scale * MB /rg.getBytesInRow());
    rg.setFilesystemHost(host);
    rgs.add(rg);
  }

  public void generateDataInParallel(int threads, int start) throws Exception{
    List<RowGenerator> splitedRg = new ArrayList<RowGenerator>();
    for (RowGenerator rg : rgs) {
      for (int i=start; i<start+threads; i++) {
        RowGenerator tmp = new RowGenerator(rg.createTableSql);
        tmp.setFilesystemHost(host);
        tmp.targetFile = storePath + "/part-" + Integer.toString(SchemaGenerator.id + start);
        SchemaGenerator.id ++;
        tmp.setExpectedRows(rg.getExpectedRows()/threads);
        splitedRg.add(tmp);
        tmp.produceRow();
      }
    }

    for (RowGenerator rg: splitedRg) {
      rg.join();
    }
  }

  public Properties loadPropertiesFromFile(String file) throws IOException {
    FileInputStream fis = new FileInputStream(file);
    Properties props = new Properties();
    props.load(fis);
    fis.close();
    return props;
  }

  public static void main(String[] args) {
    String project_root = System.getProperty("user.dir");

    try {
      SchemaGenerator sg = new SchemaGenerator();
      Properties props =  sg.loadPropertiesFromFile(project_root + "/engines/hive/conf/engineSettings.conf");
      sg.setScale(Integer.parseInt(props.getProperty("datagen.scale")));
      sg.setHost(props.getProperty("datagen.filesystem.host"));
      sg.setStorePath(props.getProperty("datagen.output.directory"));

      List<String> tables = sg.parseCreateTable(project_root + "/engines/hive/population/hiveCreateLoad.sql");

      for (String table : tables) {
        sg.addRowGenerator(table);
      }

      sg.generateDataInParallel(Integer.parseInt(props.getProperty("datagen.thread.number")),
        Integer.parseInt(props.getProperty("datagen.output.index.start")));


    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
