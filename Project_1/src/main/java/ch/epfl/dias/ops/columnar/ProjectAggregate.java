package ch.epfl.dias.ops.columnar;

import java.util.Arrays;
import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements ColumnarOperator {

	private ColumnarOperator m_child;
    private Aggregate m_agg;
    private DataType m_dataType;
    private int m_fieldNo;
    private Object m_result;
    private boolean m_eof;
    private DBColumn[] m_eofColumns;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.m_child = child;
        this.m_agg = agg;
        this.m_dataType = dt;
        this.m_fieldNo = fieldNo;
        this.m_result = null;
        this.m_eof = false;
	}

	@Override
	public DBColumn[] execute() {
		// EOF
		if (this.m_eof) {
			return this.m_eofColumns;
		}
		
		DBColumn[] childColumns = this.m_child.execute();
		
		// Construct eof columns
		this.m_eofColumns = new DBColumn[childColumns.length];
		for (int i = 0; i < childColumns.length; ++i) {
			this.m_eofColumns[i] = new DBColumn();
		}
		
		performAggregate(childColumns[this.m_fieldNo]);
		
		// Once only operator
		this.m_eof = true;
		
		return new DBColumn[] {new DBColumn(new Object[] {this.m_result}, this.m_dataType)};
	}
	

	private void performAggregate(DBColumn column)
	{
		switch (this.m_agg) {
		case SUM:
			if (this.m_dataType.equals(DataType.DOUBLE)) {
				Double[] columnValuesSum = column.getAsDouble();
				double[] columnPrimitivesSum = Arrays.stream(columnValuesSum).mapToDouble(Double::doubleValue).toArray();
				Double sum = Arrays.stream(columnPrimitivesSum).sum();
				
				this.m_result = sum;
			} else {
				Integer[] columnValuesSumInt = column.getAsInteger();
				int[] columnPrimitivesSumInt = Arrays.stream(columnValuesSumInt).mapToInt(Integer::intValue).toArray();
				Integer sumInt = Arrays.stream(columnPrimitivesSumInt).sum();
				
				this.m_result = sumInt;
			}
			break;
			
		case AVG:
			double[] columnPrimitivesAvg;
			if (column.getType().equals(DataType.DOUBLE)) {
				Double[] doubleValues = column.getAsDouble();
				columnPrimitivesAvg = Arrays.stream(doubleValues).mapToDouble(Double::doubleValue).toArray();
				
			} else {
				Integer[] intValues = column.getAsInteger();
				columnPrimitivesAvg = Arrays.stream(intValues).mapToDouble(Integer::doubleValue).toArray();
			}
			
			Double average = Arrays.stream(columnPrimitivesAvg).average().getAsDouble();
			this.m_result = average;
			break;
			
		case COUNT:
			this.m_result = column.getLength();
			break;
			
		case MAX:
			if (this.m_dataType.equals(DataType.DOUBLE)) {
				Double[] columnValuesMax = column.getAsDouble();
				double[] columnPrimitivesMax = Arrays.stream(columnValuesMax).mapToDouble(Double::doubleValue).toArray();
				Double max = Arrays.stream(columnPrimitivesMax).max().getAsDouble();	
				this.m_result = max;
			} else {
				Integer[] columnValuesMaxInt = column.getAsInteger();
				int[] columnPrimitivesMaxInt = Arrays.stream(columnValuesMaxInt).mapToInt(Integer::intValue).toArray();
				Integer maxInt = Arrays.stream(columnPrimitivesMaxInt).max().getAsInt();
				this.m_result = maxInt;
			}
				
			break;
			
		case MIN:
			if (this.m_dataType.equals(DataType.DOUBLE)) { 
				Double[] columnValuesMin = column.getAsDouble();
				double[] columnPrimitivesMin = Arrays.stream(columnValuesMin).mapToDouble(Double::doubleValue).toArray();
				Double min = Arrays.stream(columnPrimitivesMin).min().getAsDouble();
				this.m_result = min;
			} else {
				Integer[] columnValuesMinInt = column.getAsInteger();
				int[] columnPrimitivesMinInt = Arrays.stream(columnValuesMinInt).mapToInt(Integer::intValue).toArray();
				int minInt = Arrays.stream(columnPrimitivesMinInt).min().getAsInt();
				this.m_result = minInt;
			}
			
			break;
		}
		
	}
	
}
