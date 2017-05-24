package generator;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import random.DataRandom;

/**
 * Created by root on 5/24/17.
 */
public class ColumnGenerator {

    public DataRandom random;
    public String type;
    public String name;

    public ColumnGenerator (String name, ColDataType type) {
        this.name = name;
        this.type = type.getDataType();
    }

    public String nextValue() {
        if (type.equals("Integer") || type.equals("Long")) {
            random.nextLong(10);
        } else if (type.equals("Double") || type.equals("Float")) {
            return "";
        }
        return "";
    }
}
