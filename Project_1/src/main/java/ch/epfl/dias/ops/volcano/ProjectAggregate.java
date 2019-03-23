package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator m_child;
	private Aggregate m_agg;
	private DataType m_dataType;
	private int m_fieldNo;
	
	private int m_count;
	private double m_max;
	private double m_min;
	private double m_sum;
	
	private boolean m_eof;
		

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
		this.readAndAggregate();
	}

	@Override
	public DBTuple next() {
		
		if (this.m_eof) {
			// EOF
			return new DBTuple();
		}
		
		Object result = getAggregateResult();
		
		// Once only operator
		this.m_eof = true;
		
		
        return new DBTuple(new Object[] { result }, new DataType[] { this.m_dataType });
	}

	@Override
	public void close() {
		this.m_child.close();
	}
	
	private void readAndAggregate() {
		
		DBTuple currentTuple = this.m_child.next();
		
		while (!currentTuple.isEOF()) {
			
			switch(this.m_dataType) {
			case BOOLEAN:
				performAggregate(currentTuple.getFieldAsBoolean(this.m_fieldNo));
				break;
			case DOUBLE:
				performAggregate(currentTuple.getFieldAsDouble(this.m_fieldNo));
				break;
			case INT:
				performAggregate(currentTuple.getFieldAsInt(this.m_fieldNo));
				break;
			case STRING:
				performAggregate(currentTuple.getFieldAsString(this.m_fieldNo));
				break;
			}
			currentTuple = this.m_child.next();
		}
		
	}
	
	private <T> void performAggregate(T currentTupleValue)
	{
		switch (this.m_agg) {
		case SUM:
			this.m_sum += (double)currentTupleValue;
			break;
		case AVG:
			this.m_sum += (double)currentTupleValue;
			this.m_count += 1;
			break;
		case COUNT:
			this.m_count += 1;
			break;
		case MAX:
			this.m_max = Math.max(this.m_max, (double)currentTupleValue);
			break;
		case MIN:
			this.m_min = Math.min(this.m_min, (double)currentTupleValue);
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
