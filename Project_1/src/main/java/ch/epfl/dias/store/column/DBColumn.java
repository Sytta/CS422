package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	private ArrayList<Object> m_fields = new ArrayList<Object>();
	private DataType m_type;
	
	public DBColumn(Object[] fields, DataType type)
	{
		this.m_fields.add(fields);
		this.m_type = type;
	}
	
	public DBColumn(DataType type)
	{
		this.m_type = type;
	}
	
	public void addValue(Object value)
	{
		this.m_fields.add(value);
	}
	
	// Get whole column
	public Integer[] getAsInteger() {
		return (Integer[]) m_fields.toArray();
	}
	
	public Double[] getAsDouble() {
		return (Double[]) m_fields.toArray();
	}
	
	public Boolean[] getAsBoolean() {
		return (Boolean[]) m_fields.toArray();
	}
	
	public String[] getAsString() {
		return (String[]) m_fields.toArray();
	}
	
	// Get individual value
	public Object getValue(int i) {
		return m_fields.get(i);
	}
}
