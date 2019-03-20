package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ColumnStore extends Store {

	private DataType[] m_schema;
	private String m_filename;
	private String m_delimiter;
	private ArrayList<DBColumn> m_columns;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.m_schema = schema;
        this.m_filename = filename;
        this.m_delimiter = delimiter;
        this.m_columns = new ArrayList<DBColumn>();
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
        		// Read parse the value and store in corresponding columns
        		parseRowAndStore(line);
        		line = reader.readLine();
        	}
        	
        } catch (IOException e)
        {
        	e.printStackTrace();
        }
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		
		if(columnsToGet.length == 0)
		{
			// Return all columns
			return (DBColumn[]) m_columns.toArray();
		}
		
		// Deep copy
		DBColumn[] selectedColumns = new DBColumn[columnsToGet.length];
		for(int i = 0; i < columnsToGet.length; i ++)
		{
			selectedColumns[i] = m_columns.get(columnsToGet[i]);
		}
		
		return selectedColumns;
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
