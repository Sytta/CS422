package ch.epfl.dias.ops.columnar;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.OptionalDouble;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements ColumnarOperator {

	private ColumnarOperator m_child;
    private Aggregate m_agg;
    private DataType m_dataType;
    private int m_fieldNo;
    private Object m_result;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.m_child = child;
        this.m_agg = agg;
        this.m_dataType = dt;
        this.m_fieldNo = fieldNo;
        this.m_result = null;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] childColumns = this.m_child.execute();
		performAggregate(childColumns[this.m_fieldNo]);
		
		return new DBColumn[] {new DBColumn(new Object[] {this.m_result}, this.m_dataType)};
	}
	

	private void performAggregate(DBColumn column)
	{
		switch (this.m_agg) {
		case SUM:
			Double[] columnValuesSum = column.getAsDouble();
			double[] columnPrimitivesSum = Arrays.stream(columnValuesSum).mapToDouble(Double::doubleValue).toArray();
			Double sum = Arrays.stream(columnPrimitivesSum).max().getAsDouble();
			
			if (this.m_dataType.equals(DataType.DOUBLE))
				this.m_result = sum;
			else
				this.m_result = sum.intValue();
			break;
			
		case AVG:
			Double[] columnValuesAvg = column.getAsDouble();
			double[] columnPrimitivesAvg = Arrays.stream(columnValuesAvg).mapToDouble(Double::doubleValue).toArray();
			Double average = Arrays.stream(columnPrimitivesAvg).average().getAsDouble();
			this.m_result = average;
			break;
			
		case COUNT:
			this.m_result = column.getLength();
			break;
			
		case MAX:
			Double[] columnValuesMax = column.getAsDouble();
			double[] columnPrimitivesMax = Arrays.stream(columnValuesMax).mapToDouble(Double::doubleValue).toArray();
			Double max = Arrays.stream(columnPrimitivesMax).max().getAsDouble();
			
			if (this.m_dataType.equals(DataType.DOUBLE)) 
				this.m_result = max;
			else
				this.m_result = max.intValue();

			break;
			
		case MIN:
			Double[] columnValuesMin = column.getAsDouble();
			double[] columnPrimitivesMin = Arrays.stream(columnValuesMin).mapToDouble(Double::doubleValue).toArray();
			Double min = Arrays.stream(columnPrimitivesMin).min().getAsDouble();
			
			if (this.m_dataType.equals(DataType.DOUBLE)) 
				this.m_result = min;
			else 
				this.m_result = min.intValue();
			
			break;
		}
		
	}
	
	
}
