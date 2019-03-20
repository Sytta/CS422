package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator m_child;
	private BinaryOp m_op;
	private int m_fieldNo;
	private int m_selectValue;
	
	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.m_child = child;
		this.m_op = op;
		this.m_fieldNo = fieldNo;
		this.m_selectValue = value;
	}

	@Override
	public void open() {
		this.m_child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple currentTuple = this.m_child.next();
		boolean tupleFound = false;
		
		while (!currentTuple.isEOF()) {
			
			tupleFound = isOpSatisfied(currentTuple.getFieldAsInt(this.m_fieldNo));
			
			if (tupleFound) {
				return currentTuple;
			}
			
			currentTuple = this.m_child.next();
		}
		
		
		return currentTuple;
		
	}

	@Override
	public void close() {
		this.m_child.close();
	}
	
	private boolean isOpSatisfied(int currentTupleValue) {
		
		switch(this.m_op) {
		case EQ:
			return this.m_selectValue == currentTupleValue;
		case GE:
			return currentTupleValue >= this.m_selectValue;
		case GT:
			return currentTupleValue > this.m_selectValue;
		case LE:
			return currentTupleValue <= this.m_selectValue;
		case LT:
			return currentTupleValue < this.m_selectValue;
		case NE:
			return currentTupleValue != this.m_selectValue;
		default:
			System.err.printf("Select::isOpSatisfied: default case with op %s \n", this.m_op);
			return false;
		}
		
	}
}
