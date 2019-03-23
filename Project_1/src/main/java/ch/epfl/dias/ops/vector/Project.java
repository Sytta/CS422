package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	private VectorOperator m_child;
	private int[] m_fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.m_child = child;
		this.m_fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.m_child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] childVector = this.m_child.next();
		// EOF
		if (childVector[0].isEOF()) {
			return childVector;	
		} 
		
		DBColumn[] resultVector = new DBColumn[this.m_fieldNo.length];
		
		for (int i = 0; i < this.m_fieldNo.length; ++i) {
			resultVector[i] = new DBColumn(childVector[i]);
		}
		
		return resultVector;
	}

	@Override
	public void close() {
		this.m_child.close();
	}

}
