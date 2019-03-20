package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	private DataType[] m_schema;
	private String m_filename;
	private String m_delimiter;
	private ArrayList<DBTuple> m_tuples;


	public RowStore(DataType[] schema, String filename, String delimiter) {
        this.m_schema = schema;
        this.m_filename = filename;
        this.m_delimiter = delimiter;
        this.m_tuples = new ArrayList<DBTuple>();
	}

	@Override
	public void load() throws IOException {
        String rootPath = System.getProperty("user.dir");
        String filePath = rootPath + "/" + m_filename;
        BufferedReader reader;
        try {
        	reader = new BufferedReader(new FileReader(filePath));
        	String line = reader.readLine();
        	while(line != null)
        	{
        		DBTuple tuple = parseRow(line);
        		this.m_tuples.add(tuple);
        		line = reader.readLine();
        	}
        	
        } catch (IOException e)
        {
        	e.printStackTrace();
        }
    }

	@Override
	public DBTuple getRow(int rownumber) {
		try {
			return m_tuples.get(rownumber);
		} catch(IndexOutOfBoundsException e) {
			System.err.printf("RowStore::getRow: Index Out of bound; rownumber = %d", rownumber);
			return new DBTuple();
		}
	}
	
	private DBTuple parseRow(String line) {
		Object[] parsedValues = new Object[m_schema.length];
		String[] inputs = line.split(m_delimiter);
		
		for(int i = 0; i < m_schema.length; ++i)
		{
			switch (m_schema[i]) {
			case INT:
				parsedValues[i] = Integer.parseInt(inputs[i]);
				break;
			case BOOLEAN:
				parsedValues[i] = Boolean.parseBoolean(inputs[i]);
				break;
			case DOUBLE:
				parsedValues[i] = Double.parseDouble(inputs[i]);
				break;
			case STRING:
				parsedValues[i] = inputs[i];
				break;
			}
		}

		return new DBTuple(parsedValues, m_schema);
	}
}
