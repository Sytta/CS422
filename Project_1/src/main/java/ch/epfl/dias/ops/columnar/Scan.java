package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements ColumnarOperator {

	private ColumnStore m_store;

	public Scan(ColumnStore store) {
		this.m_store = store;
	}

	@Override
	public DBColumn[] execute() {
		// return all columns / all indexes
		return this.m_store.getColumns(new int[] {});
	}
	
}
