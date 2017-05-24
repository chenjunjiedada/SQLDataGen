package generator;

import com.sun.jersey.api.ParamException;
import database.Schema;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.List;

public class RowGenerator {
    private List<ColumnGenerator> cgs;
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

    public String nextRow() {
        String output = "";
        for (ColumnGenerator cg: cgs) {
            output += cg.nextValue();
            output += "|";
        }
        return output;
    }
}
