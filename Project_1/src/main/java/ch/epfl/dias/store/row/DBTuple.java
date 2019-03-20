package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;

public class DBTuple {
	private Object[] m_fields;
	private DataType[] m_types;
	private boolean m_eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.m_fields = fields;
		this.m_types = types;
		
		this.m_eof = false;
	}

	public DBTuple() {
		// Last tuple in the table
		this.m_eof = true;
	}
	
	public boolean isEOF() {
		return this.m_eof;
	}
	
	public DataType getFieldType(int fieldNo) {
		return this.m_types[fieldNo];
	}

	/**
	 * XXX Assuming that the caller has ALREADY checked the datatype, and has
	 * made the right call
	 * 
	 * @param fieldNo
	 *            (starting from 0)
	 * @return cast of field
	 */
	public Integer getFieldAsInt(int fieldNo) {
		return (Integer) m_fields[fieldNo];
	}

	public Double getFieldAsDouble(int fieldNo) {
		return (Double) m_fields[fieldNo];
	}

	public Boolean getFieldAsBoolean(int fieldNo) {
		return (Boolean) m_fields[fieldNo];
	}

	public String getFieldAsString(int fieldNo) {
		return (String) m_fields[fieldNo];
	}
	
	public Object getFieldAsObject(int fieldNo) {
		return m_fields[fieldNo];
	}
}
