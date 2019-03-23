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
		
		for(Object val: fields) {
			this.m_fields.add(val);
		}
		
		this.m_type = type;
	}
	
	public DBColumn(DataType type)
	{
		this.m_fields = new ArrayList<Object>();
		this.m_type = type;
		this.m_EOF = true;
	}
	
	public DBColumn(DBColumn col) {
		this.m_fields = col.m_fields;
		this.m_type = col.m_type;
		this.m_EOF = col.m_EOF;
	}
	
	public<T> void addValue(T value)
	{
		this.m_fields.add(value);
		if (this.m_EOF) {
			this.m_EOF = false;
		}
	}
	
	public void addValues(DBColumn col) {
		this.m_fields.addAll(col.m_fields);
		if (this.m_EOF) {
			this.m_EOF = false;
		}
	}
	
	public boolean isEOF() {
		return this.m_EOF;
	}
	
	public int getLength() {
		return this.m_fields.size();
	}
	
	public DataType getType() {
		return this.m_type;
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
	
	public Object[] getAsObject() {
		return m_fields.toArray(new Object[this.m_fields.size()]);
	}
	
	// Get individual value
	public Object getValue(int i) {
		return m_fields.get(i);
	}
	
	// Functions for operations
	public DBColumn selectRows(ArrayList<Integer> selectedRowIndex) {
		int i = 0;
		Object[] selectedValues = new Object[selectedRowIndex.size()];
		for (Integer rowIndex : selectedRowIndex) {
			selectedValues[i++] = this.m_fields.get(rowIndex);
		}
		return new DBColumn(selectedValues, this.m_type);
	}
	
}
