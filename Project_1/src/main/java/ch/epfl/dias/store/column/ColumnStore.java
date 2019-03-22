package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	private DataType[] m_schema;
	private String m_filename;
	private String m_delimiter;
	private ArrayList<DBColumn> m_columns;
	private boolean m_lateMaterialize;
	
	private int m_nbRows;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.m_schema = schema;
        this.m_filename = filename;
        this.m_delimiter = delimiter;
        this.m_columns = new ArrayList<DBColumn>();
        this.m_nbRows = 0;
        this.m_lateMaterialize = lateMaterialization;
	}

	@Override
	public void load() throws IOException {
		
		// Construct the empty data columns according to schema
		for (int i = 0; i < m_schema.length; ++i)
		{
			m_columns.add(new DBColumn(m_schema[i]));
		}
		
		String rootPath = System.getProperty("user.dir");
        String filePath = rootPath + "/" + m_filename;
        BufferedReader reader;
        try {
        	reader = new BufferedReader(new FileReader(filePath));
        	String line = reader.readLine();
        	while(line != null)
        	{
        		++this.m_nbRows;
        		// Read parse the value and store in corresponding columns
        		parseRowAndStore(line);
        		line = reader.readLine();
        	}
        	
        	System.out.printf("%d rows read from file %s", this.m_nbRows, this.m_filename);
        	
        } catch (IOException e)
        {
        	e.printStackTrace();
        }
	}
	
	public DBColumn getColumn(int columnToGet) {
		return this.m_columns.get(columnToGet);
	}
	
	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		
		if (this.m_lateMaterialize) {
			return getColumnIndex(columnsToGet);
		} else {
			return getColumnValue(columnsToGet);
		}
	}
	
	// Early materialization - return values
	public DBColumn[] getColumnValue(int [] columnsToGet) {
		if(columnsToGet.length == 0)
		{
			// Return all columns
			return m_columns.toArray(new DBColumn[0]);
		}
		
		// Deep copy
		DBColumn[] selectedColumns = new DBColumn[columnsToGet.length];
		for(int i = 0; i < columnsToGet.length; ++i)
		{
			selectedColumns[i] = m_columns.get(columnsToGet[i]);
		}
		
		return selectedColumns;
	}
	
	// Late materialization - only return ids
	public DBColumn[] getColumnIndex(int[] columnsToGet) {
		// if columnsToGet is empty, return indexes for all columns
		int[] colNos;
		
		if (columnsToGet.length == 0) {
			colNos = new int[this.m_schema.length];
			// Construct column ids
			for(int i = 0; i < colNos.length; i++) {
				colNos[i] = i;
			}
		} else {
			colNos = columnsToGet;
		}
		int[] rowNos = new int[this.m_nbRows];
		DBColumnId[] columnIndexes = new DBColumnId[colNos.length];
		
		// Construct row ids
		for(int i = 0; i < this.m_nbRows; i++) {
			rowNos[i] = i;
		}
		
		for(int i = 0; i <  colNos.length; ++i) {
			columnIndexes[i] = new DBColumnId(this, rowNos, colNos[i]);
		}
		
		return columnIndexes;
	}
	
	
	private void parseRowAndStore(String line) {
		Object parsedValue;
		String[] inputs = line.split(m_delimiter);
		
		for(int i = 0; i < m_schema.length; ++i)
		{
			switch (m_schema[i]) {
			case INT:
				parsedValue = Integer.parseInt(inputs[i]);
				m_columns.get(i).addValue(parsedValue);
				break;
			case BOOLEAN:
				parsedValue = Boolean.parseBoolean(inputs[i]);
				m_columns.get(i).addValue(parsedValue);
				break;
			case DOUBLE:
				parsedValue = Double.parseDouble(inputs[i]);
				m_columns.get(i).addValue(parsedValue);
				break;
			case STRING:
				parsedValue = inputs[i];
				m_columns.get(i).addValue(parsedValue);
				break;
			}
		}
		
	}
}
