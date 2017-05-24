package generator;

import java.util.ArrayList;
import java.util.List;


public class SchemaGenerator {
    private List<RowGenerator> rgs = new ArrayList<RowGenerator>();
    public int scale = 1;

    public int addRowGenerator(RowGenerator rg) {
        rgs.add(rg);
        return 0;
    }

    public int setScale(int scale) {
        this.scale = scale;
        return 0;
    }


    public void generateData(String dir) throws Exception {
        // how to set expected rows for each table
        for(RowGenerator rg : rgs) {
            rg.generateData(dir);
        }
    }

    public static void main(String[] args) {
        String sql = "create table test (a int, b STRING);";
        try {
            SchemaGenerator sg = new SchemaGenerator();
            sg.addRowGenerator(new RowGenerator(sql));
            sg.generateData("/tmp/testdata");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
