package ch.epfl.dias.store.PAX;

import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	private DBColumn[] m_minipages;
	private DataType[] m_types;
	private int m_pageNo;
	
	public DBPAXpage(DataType[] types, int pageNo) {
		this.m_types = types;
		this.m_pageNo = pageNo;
		this.m_minipages = new DBColumn[types.length];
		
		// Initialize each minipage with corresponding type
		for(int i = 0; i < this.m_types.length; ++i) {
			this.m_minipages[i] = new DBColumn(this.m_types[i]);
		}
	}
	
	public int getPageNumber()
	{
		return this.m_pageNo;
	}
	
	// Add values to columns (minipages) according to type
	public <T> void addValue(T value, int typeIndex) {
		this.m_minipages[typeIndex].addValue(value);
	}
	
	
	// Get row
	public DBTuple getTuple(int rowNo) {
		Object[] fields = new Object[this.m_types.length];
		
		for(int i =0; i < this.m_types.length; ++i)
		{
			fields[i] = this.m_minipages[i].getValue(rowNo);
		}
		
		return new DBTuple(fields, this.m_types);
	}
	
}
