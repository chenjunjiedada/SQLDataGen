package generator;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class RowGenerator {
    private long expectedRows = 1;
    private List<ColumnGenerator> cgs = new ArrayList<ColumnGenerator>();
    private CreateTable createTableStat;

    public RowGenerator(String createTableSql) throws Exception {
        Statement st = CCJSqlParserUtil.parse(createTableSql);
        if (!(st instanceof CreateTable)) {
            throw new RuntimeException("Not a valid create table SQL");
        }
        this.createTableStat = (CreateTable)st;

        for(ColumnDefinition columnDefinition: createTableStat.getColumnDefinitions()) {
            ColumnGenerator cg = new ColumnGenerator(columnDefinition.getColumnName(), columnDefinition.getColDataType());
            cgs.add(cg);
        }
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

        for (long i = 0; i < expectedRows; i++) {
            os.write(nextRow().getBytes());
        }
        os.close();
        hdfs.close();
        return 0;
    }

    public String nextRow() {
        String output = "";
        for (ColumnGenerator cg: cgs) {
            output += cg.nextValue();
            output += "|";
        }
        return output;
    }
}
