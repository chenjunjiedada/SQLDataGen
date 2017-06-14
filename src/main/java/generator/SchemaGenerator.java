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
  public long scale = 1;
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
    rg.setTargetPath(storePath);
    rgs.add(rg);
  }

  public void generateData() throws Exception {
    for (RowGenerator rg : rgs) {
      rg.produceRow();
    }
  }

  public void generateDataInParallel(){
    //TODO
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
      Properties props =  sg.loadPropertiesFromFile(project_root + "/conf/engineSettings.conf");
      sg.setScale(Integer.parseInt(props.getProperty("datagen.scale")));
      sg.setHost(props.getProperty("datagen.filesystem.host"));
      sg.setStorePath(props.getProperty("datagen.output.directory"));

      List<String> tables = sg.parseCreateTable(project_root + "/engines/hive/population/hiveCreateLoad.sql");

      for (String table : tables) {
        sg.addRowGenerator(table);
      }

      sg.generateData();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
