package ch.epfl.dias.store.column;

import java.util.ArrayList;

public class DBColumnId extends DBColumn {
	
	private ColumnStore m_columnStorePtr;
	private int[] m_rowIndex;
	
	private int m_colIndex;
	
	public DBColumnId() {
		this.m_EOF = true;
	}
	
	public DBColumnId(ColumnStore columnStore, int[] rowIndex, int currentCol)
	{
		this.m_columnStorePtr = columnStore;
		this.m_rowIndex = rowIndex;
		this.m_colIndex = currentCol;
	}
	
	public DBColumnId(DBColumnId copy) {
		this.m_columnStorePtr = copy.m_columnStorePtr;
		this.m_rowIndex = copy.m_rowIndex;
		this.m_colIndex = copy.m_colIndex;
	}
	
	
	public void setRowIndex(int[] newRowIndex) {
		this.m_rowIndex = newRowIndex;
	}
	
	public int[] getRowIndex() {
		return this.m_rowIndex;
	}
	
	public int getColumnIndex() {
		return this.m_colIndex;
	}
	
	@Override
	public int getLength() {
		return this.m_rowIndex.length;
	}
	

	@Override
	public Integer[] getAsInteger() {
		DBColumn column = this.m_columnStorePtr.getColumn(m_colIndex);
		// Filter value
		Integer[] filteredColumn = new Integer[this.m_rowIndex.length];
		
		for (int i = 0; i < this.m_rowIndex.length; ++i) {
			filteredColumn[i] = (Integer)column.getValue(m_rowIndex[i]);
		}
		
		return filteredColumn;
	}
	
	@Override
	public Double[] getAsDouble() {
		DBColumn column = this.m_columnStorePtr.getColumn(m_colIndex);
		// Filter value
		Double[] filteredColumn = new Double[this.m_rowIndex.length];
		
		for (int i = 0; i < this.m_rowIndex.length; ++i) {
			filteredColumn[i] = (Double)column.getValue(m_rowIndex[i]);
		}
		
		return filteredColumn;
	}
	
	@Override
	public Boolean[] getAsBoolean() {
		DBColumn column = this.m_columnStorePtr.getColumn(m_colIndex);
		// Filter value
		Boolean[] filteredColumn = new Boolean[this.m_rowIndex.length];
		
		for (int i = 0; i < this.m_rowIndex.length; ++i) {
			filteredColumn[i] = (Boolean)column.getValue(m_rowIndex[i]);
		}
		
		return filteredColumn;
	}
	
	@Override
	public String[] getAsString() {
		DBColumn column = this.m_columnStorePtr.getColumn(m_colIndex);
		// Filter value
		String[] filteredColumn = new String[this.m_rowIndex.length];
		
		for (int i = 0; i < this.m_rowIndex.length; ++i) {
			filteredColumn[i] = (String)column.getValue(m_rowIndex[i]);
		}
		
		return filteredColumn;
	}
	
	@Override
	public Object[] getAsObject() {
		DBColumn column = this.m_columnStorePtr.getColumn(m_colIndex);
		// Filter value
		Object[] filteredColumn = new Object[this.m_rowIndex.length];
		
		for (int i = 0; i < this.m_rowIndex.length; ++i) {
			filteredColumn[i] = column.getValue(m_rowIndex[i]);
		}
		
		return filteredColumn;
	}
	
	@Override
	public DBColumn selectRows(ArrayList<Integer> selectedRowIndex) {
		int[] selectedValues = new int[selectedRowIndex.size()];
		int i = 0;
		for (Integer row : selectedRowIndex) {
			selectedValues[i++] = this.m_rowIndex[row];
		}
		
		return new DBColumnId(this.m_columnStorePtr, selectedValues, this.m_colIndex);
	}

}
