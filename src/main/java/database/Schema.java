package database;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.HashMap;
import java.util.List;

public class Schema {
    public List<CreateTable> tables;

    public HashMap<String, String> properties;

    public Schema() {
        
    }

    public int addTable(String sql) throws JSQLParserException {
        Statement st = CCJSqlParserUtil.parse(sql);
        if (!(st instanceof CreateTable)) {
            return -1;
        }
        tables.add((CreateTable)st);
        return 0;
    }
}
