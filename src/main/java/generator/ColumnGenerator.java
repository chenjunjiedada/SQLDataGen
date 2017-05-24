package generator;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import random.DataRandom;

/**
 * Created by root on 5/24/17.
 */
public class ColumnGenerator {

    public DataRandom random = new DataRandom();
    public String type;
    public String name;

    public ColumnGenerator (String name, ColDataType type) {
        this.name = name;
        this.type = type.getDataType();
    }

    public String nextValue() {
        if (type.toLowerCase().equals("int") || type.toLowerCase().equals("long")) {
            // TODO: decide the long scale.
            return random.nextLong(10);
        } else if (type.toLowerCase().equals("double") || type.toLowerCase().equals("float")) {
            return "";
        } else if (type.toLowerCase().startsWith("decimal")) {
            // TODO, parse the scale and precision
            return random.nextDecimal(10, 2);
        } else if (type.toLowerCase().equals("string")) {
            return random.nextString();
        }

        return "";

    }
}
