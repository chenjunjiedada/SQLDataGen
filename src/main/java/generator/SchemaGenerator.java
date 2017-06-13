package generator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SchemaGenerator {
  private List<RowGenerator> rgs = new ArrayList<RowGenerator>();
  public long scale = 1;
  public int addRowGenerator(RowGenerator rg) {
    rgs.add(rg);
    return 0;
  }

  public int setScale(int scale) {
    this.scale = scale;
    return 0;
  }

  public List<String> parseCreateTable(String sqlFile) throws IOException {
    FileReader fileReader = new FileReader(sqlFile);
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    List<String> sqls = new ArrayList<String>();

    String query = "";
    String line;
    while ( (line = bufferedReader.readLine()) != null) {
      query = query + line +"\n";
      if (line.contains(";")) {
        if(query.toLowerCase().contains("create external")) {
          sqls.add(query.substring(0, query.lastIndexOf(')') + 1) + ";");
        }
        query = "";
      }
    }

    return sqls;
  }


  public void generateData(Properties props) throws Exception {
    // how to set expected rows for each table
    for (RowGenerator rg : rgs) {
      rg.generateData(props);
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
      List<String> createTableSqls = sg.parseCreateTable(project_root + "/engines/hive/population/hiveCreateLoad.sql");

      RowGenerator rg = new RowGenerator(createTableSqls.get(0));
      rg.setExpectedRows(sg.scale * 1024 * 1024 / rg.getBytesInRow());
      sg.addRowGenerator(rg);

      sg.generateData(sg.loadPropertiesFromFile(project_root + "/conf/datagen.properties"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
