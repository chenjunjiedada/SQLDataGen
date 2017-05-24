package generator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.util.List;


public class SchemaGenerator {
    private List<RowGenerator> rgs;
    public int scale = 1;

    public int addRowGenerator(RowGenerator rg) {
        rgs.add(rg);
        return 0;
    }

    public int setScale(int scale) {
        this.scale = scale;
        return 0;
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
        // TODO write data here.
        os.close();
        hdfs.close();
        return 0;
    }

    public static void main(String[] args) {
        String sql = "create table (a int, b string)";
        try {
            SchemaGenerator sg = new SchemaGenerator();
            sg.addRowGenerator(new RowGenerator(sql));
            sg.generateData("/tmp/testdata");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
