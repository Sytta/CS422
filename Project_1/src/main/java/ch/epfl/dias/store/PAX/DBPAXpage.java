package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	private DBColumn m_intMinipage =  new DBColumn(DataType.INT);
	private DBColumn m_doubleMinipage = new DBColumn(DataType.DOUBLE);
	private DBColumn m_boolMinipage = new DBColumn(DataType.BOOLEAN);
	private DBColumn m_stringMinipage = new DBColumn(DataType.STRING);
	private DataType[] m_types;
	private int m_pageNo;
	
	public DBPAXpage(DataType[] types, int pageNo) {
		this.m_types = types;
		this.m_pageNo = pageNo;
	}
	
	public int getPageNumber()
	{
		return this.m_pageNo;
	}
	
	// Add values to columns (minipages) according to type
	public void addValue(int value) {
		this.m_intMinipage.addValue(value);
	}
	
	public void addValue(double value) {
		this.m_doubleMinipage.addValue(value);
	}
	
	public void addValue(boolean value) {
		this.m_boolMinipage.addValue(value);
	}
	
	public void addValue(String value) {
		this.m_stringMinipage.addValue(value);
	}
	
	// Get row
	public DBTuple getTuple(int rowNo) {
		Object[] fields = new Object[this.m_types.length];
		
		for(int i =0; i < this.m_types.length; ++i)
		{
			switch(this.m_types[i])
			{
			case INT:
				fields[i] = this.m_intMinipage.getValue(i);
				break;
			case BOOLEAN:
				fields[i] = this.m_boolMinipage.getValue(i);
				break;
			case DOUBLE:
				fields[i] = this.m_doubleMinipage.getValue(i);
				break;
			case STRING:
				fields[i] = this.m_stringMinipage.getValue(i);
				break;
			}
		}
		
		return new DBTuple(fields, this.m_types);
	}
	
}
