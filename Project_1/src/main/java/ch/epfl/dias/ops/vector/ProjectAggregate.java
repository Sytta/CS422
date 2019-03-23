package ch.epfl.dias.ops.vector;

import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	VectorOperator m_child;
	Aggregate m_agg;
	DataType m_dataType;
	int m_fieldNo;
	
	private int m_count;
	private double m_max;
	private double m_min;
	private double m_sum;
	
	private boolean m_eof;
	private DBColumn[] m_eofColumns;
	
	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.m_child = child;
		this.m_agg = agg;
		this.m_dataType = dt;
		this.m_fieldNo = fieldNo;
		
		this.m_count = 0;
		this.m_max = -Double.MAX_VALUE;
		this.m_min = Double.MAX_VALUE;
		this.m_sum = 0;
		this.m_eof = false;
	}

	@Override
	public void open() {
		this.m_child.open();
		
		DBColumn[] currentChildVector = this.m_child.next();
		while(!currentChildVector[0].isEOF()) {
			performAggregate(currentChildVector[this.m_fieldNo]);
			currentChildVector = this.m_child.next();
		}
		
		this.m_eofColumns = new DBColumn[currentChildVector.length];
		
		for (int i = 0; i < currentChildVector.length; ++i) {
			this.m_eofColumns[i] = new DBColumn();
		}
		
	}

	@Override
	public DBColumn[] next() {
		if (this.m_eof) {
			return this.m_eofColumns;
		}
		
		Object result = getAggregateResult();
		
		// Once only operator
		if (!this.m_eof) {
			this.m_eof = true;
		}
        return new DBColumn[] {new DBColumn(new Object[] { result }, this.m_dataType)};
	}

	@Override
	public void close() {
		this.m_child.close();
	}
	
	private void performAggregate(DBColumn column)
	{
		switch (this.m_agg) {
		case SUM:
		case AVG:
			Double[] columnValuesSum = column.getAsDouble();
			double[] columnPrimitivesSum = Arrays.stream(columnValuesSum).mapToDouble(Double::doubleValue).toArray();
			Double sum = Arrays.stream(columnPrimitivesSum).max().getAsDouble();
			this.m_sum += sum;
			break;
			
		case COUNT:
			this.m_count += column.getLength();
			break;
			
		case MAX:
			Double[] columnValuesMax = column.getAsDouble();
			double[] columnPrimitivesMax = Arrays.stream(columnValuesMax).mapToDouble(Double::doubleValue).toArray();
			Double max = Arrays.stream(columnPrimitivesMax).max().getAsDouble();
			
			this.m_max = Math.max(this.m_max, max);
			
			break;
			
		case MIN:
			Double[] columnValuesMin = column.getAsDouble();
			double[] columnPrimitivesMin = Arrays.stream(columnValuesMin).mapToDouble(Double::doubleValue).toArray();
			Double min = Arrays.stream(columnPrimitivesMin).min().getAsDouble();
			
			this.m_min = Math.max(this.m_min, min);

			break;
		}
		
	}
	
	private Object getAggregateResult() {
		switch (this.m_agg) {
		case SUM:
			return this.m_dataType == DataType.DOUBLE ? (Double)this.m_sum : new Integer((int) this.m_sum);
		case AVG: 
			return this.m_sum / this.m_count; // always double
		case COUNT: 
			return this.m_count; // always int
		case MAX:
			return this.m_dataType == DataType.DOUBLE ? (Double)this.m_max : new Integer((int) this.m_max);
		case MIN:
			return this.m_dataType == DataType.DOUBLE ? (Double)this.m_min : new Integer((int) this.m_min);
		}
		return null;
	}

}
