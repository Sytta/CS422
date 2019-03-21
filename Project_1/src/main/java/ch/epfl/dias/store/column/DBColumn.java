package ch.epfl.dias.store.column;

import java.util.ArrayList;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	
	private ArrayList<Object> m_fields;
	private DataType m_type;
	protected boolean m_EOF = false;
	
	public DBColumn() {
		this.m_fields = null;
		this.m_type = null;
		this.m_EOF = true;
	}
	
	public DBColumn(Object[] fields, DataType type)
	{
		this.m_fields = new ArrayList<Object>();
		this.m_fields.add(fields);
		this.m_type = type;
	}
	
	public DBColumn(DataType type)
	{
		this.m_fields = new ArrayList<Object>();
		this.m_type = type;
	}
	
	public<T> void addValue(T value)
	{
		this.m_fields.add(value);
	}
	
	public boolean isEOF() {
		return this.m_EOF;
	}
	
	// Get whole column
	public Integer[] getAsInteger() {
		return m_fields.toArray(new Integer[this.m_fields.size()]);

	}
	
	public Double[] getAsDouble() {
		return m_fields.toArray(new Double[this.m_fields.size()]);
	}
	
	public Boolean[] getAsBoolean() {
		return m_fields.toArray(new Boolean[this.m_fields.size()]);
	}
	
	public String[] getAsString() {
		return m_fields.toArray(new String[this.m_fields.size()]);
	}
	
	// Get individual value
	public Object getValue(int i) {
		return m_fields.get(i);
	}
	
	// Functions for operations
	public DBColumn selectRows(ArrayList<Integer> selectedRowIndex) {
		
		Object[] selectedValues;
		
		switch(this.m_type) {
		case BOOLEAN:
			selectedValues = new Boolean[selectedRowIndex.size()];
			break;
		case DOUBLE:
			selectedValues = new Double[selectedRowIndex.size()];
			break;
		case INT:
			selectedValues = new Integer[selectedRowIndex.size()];
			break;
		case STRING:
			selectedValues = new String[selectedRowIndex.size()];
			break;
		default:
			selectedValues = new Object[selectedRowIndex.size()];
			break;
		}
						
		int i = 0;
		for (Integer rowIndex : selectedRowIndex) {
			selectedValues[i++] = this.m_fields.get(rowIndex);
		}
		
		return new DBColumn(selectedValues, this.m_type);
	}
}
