package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store m_store; // NSM or PAX
	private int m_currentRowIndex;

	public Scan(Store store) {
		this.m_store = store;
	}

	@Override
	public void open() {
		this.m_currentRowIndex = 0;
	}

	@Override
	public DBTuple next() {
		return this.m_store.getRow(this.m_currentRowIndex ++);
	}

	@Override
	public void close() {
		this.m_currentRowIndex = 0;
	}
}