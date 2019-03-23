package ch.epfl.dias.ops.vector;

import java.util.ArrayList;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	private Store m_store;
	private int m_vectorsize;
	private int m_rowStartIndex;
	private DBColumn[] m_columns;
	private int m_columnLength;

	public Scan(Store store, int vectorsize) {
		this.m_store = store;
		this.m_vectorsize = vectorsize;
		this.m_rowStartIndex = 0;
		this.m_columnLength = 0;
	}
	
	public int getVectorSize() {
		return this.m_vectorsize;
	}
	
	@Override
	public void open() {
		this.m_rowStartIndex = 0;
		this.m_columns = this.m_store.getColumns(new int[] {});
		this.m_columnLength = this.m_columns[0].getLength();
	}

	@Override
	public DBColumn[] next() {
		// EOF
		if (this.m_rowStartIndex >= this.m_columnLength) {
			DBColumn[] eofColumns = new DBColumn[this.m_columns.length];
			for (int i = 0; i < this.m_columns.length; ++i) {
				eofColumns[i] = new DBColumn();
			}
			return eofColumns;
		}
		
		// Build the indexes
		int endRowIndex = this.m_rowStartIndex + this.m_vectorsize > m_columnLength ? m_columnLength : this.m_rowStartIndex + this.m_vectorsize; 
		ArrayList<Integer> selectedRows = new ArrayList<Integer>();

		for(int i = this.m_rowStartIndex; i < endRowIndex; ++i) {
			selectedRows.add(i);
		}
		
		// Fetch corresponding rows
		DBColumn[] nextColumns = new DBColumn[this.m_columns.length];
		for (int i = 0; i < this.m_columns.length; ++i) {
			nextColumns[i] = this.m_columns[i].selectRows(selectedRows);
		}
		
		// Set rowStartIndex for next fetch
		this.m_rowStartIndex = endRowIndex;
		
		return nextColumns;
		
	}

	@Override
	public void close() {
		this.m_rowStartIndex = 0;
		this.m_vectorsize = 0;
		this.m_columns = null;
		this.m_store = null;
		
	}
}
