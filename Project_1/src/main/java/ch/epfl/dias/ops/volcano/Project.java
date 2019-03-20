package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	VolcanoOperator m_child;
	int[] m_fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.m_child = child;
		this.m_fieldNo = fieldNo;
	}

	@Override
	public void open() {
		m_child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple currentTuple = this.m_child.next();
		DataType[] returnDataTypes = new DataType[this.m_fieldNo.length];
		Object[] returnFields = new Object[this.m_fieldNo.length];
		
		// Only choose the interested fields
		for(int i = 0; i < this.m_fieldNo.length; ++i) {
			returnDataTypes[i] = currentTuple.getFieldType(this.m_fieldNo[i]);
			returnFields[i] = currentTuple.getFieldAsObject(this.m_fieldNo[i]);
		}
		
		// To avoid returning an empty tuple that is not EOF
		return currentTuple.isEOF() ? currentTuple : new DBTuple(returnFields, returnDataTypes);
		
	}

	@Override
	public void close() {
		m_child.close();
	}
}
