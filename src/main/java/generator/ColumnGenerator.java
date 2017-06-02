package generator;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.apache.commons.lang.math.RandomUtils;
import random.DataRandom;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Random;

/**
 * Created by root on 5/24/17.
 */
public class ColumnGenerator {
    public final int defaultScale = 10;
    public final int defaultPrecision = 6;

    public DataRandom random = new DataRandom();

    public ColumnDefinition colDesc;
    public double nullProportion = 0.0;

    public ColumnGenerator () {

    }
    public ColumnGenerator (ColumnDefinition colDescriptor) {
        this.colDesc = colDescriptor;
    }
    public void setColDesc(ColumnDefinition cd) {
        this.colDesc = cd;
    }

    public void setNullProportion(double proportion) throws InvalidParameterException {
        if (proportion > 0.0 && proportion < 1.0) {
            this.nullProportion = proportion;
        } else {
            throw new InvalidParameterException("proportion must less than 1.0");
        }
    }

    public boolean isPartitionColumn(){
        return false;
    }

    /*
        Generate random value in string according to column type.
        It also has to consider the null proportion.
     */
    public String nextValue() {
        if (nullProportion > 0.0) {
            //TODO
            if(Math.random()<nullProportion){
                return null;
            }
        }
        String type = colDesc.getColDataType().getDataType();
        if (type.toLowerCase().equals("int") || type.toLowerCase().equals("long")) {
            return random.nextLong();
        } else if (type.toLowerCase().equals("double") || type.toLowerCase().equals("float")) {
            return random.nextDouble();
        } else if (type.toLowerCase().startsWith("decimal")) {
            List<String> params = colDesc.getColDataType().getArgumentsStringList();
            int scale = Integer.getInteger(params.get(0), defaultScale);
            int precision = Integer.getInteger(params.get(1), defaultPrecision);
            return random.nextDecimal(scale, precision);
        } else if (type.toLowerCase().equals("string")) {
            return random.nextString();
        } else if (type.toLowerCase().equals("timestamp")) {
            return random.nextTimestamp();
        }

        return "";

    }
}
