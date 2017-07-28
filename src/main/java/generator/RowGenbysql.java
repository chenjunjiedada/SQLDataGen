package generator;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by root on 7/27/17.
 */
public class RowGenbysql {
    private List<ColumnGenbysql> cgs = new ArrayList<>();
    private CreateTable createTableStat;
    private HashMap<String,Double> distinctProportion;
    private HashMap<String,Double> nullProportion;
    private String createTableSql;

    public RowGenbysql(String sql) throws Exception{
        createTableSql = sql;
        distinctProportion = new HashMap<>();
        nullProportion = new HashMap<>();
        Statement st = CCJSqlParserUtil.parse(createTableSql);
        if (!(st instanceof CreateTable)) {
            throw new RuntimeException("Not a valid create table SQL");
        }
        this.createTableStat = (CreateTable) st;
    }

    public void setDistinctProportion(HashMap<String, Double> distinctProportion) {
        this.distinctProportion = distinctProportion;
    }

    public void setNullProportion(HashMap<String,Double> nullProportion) {
        this.nullProportion = nullProportion;
    }

    public void setColumnGenbysql(){
        for (ColumnDefinition columnDefinition : createTableStat.getColumnDefinitions()) {
            ColumnGenbysql cg = new ColumnGenbysql(columnDefinition);
            String columnName = columnDefinition.getColumnName();
            if (nullProportion.containsKey(columnName.toLowerCase())){
                cg.setNullProportion(nullProportion.get(columnName.toLowerCase()));
            }
            if (distinctProportion.containsKey(columnName.toLowerCase())){
                cg.setDistinctProportion(distinctProportion.get(columnName.toLowerCase()));
            }
            cgs.add(cg);
        }
    }

    public String nextRow() {
        String output = "";
        for (ColumnGenbysql cg : cgs) {
            output += cg.nextValue();
            output += "|";
        }
        return output;
    }
}
