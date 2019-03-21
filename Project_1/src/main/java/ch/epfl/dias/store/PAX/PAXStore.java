package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	private DataType[] m_schema;
	private String m_filename;
	private String m_delimiter;
	private int m_tuplesPerPage;
	private ArrayList<DBPAXpage> m_pages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.m_schema = schema;
        this.m_filename = filename;
        this.m_delimiter = delimiter;
        this.m_tuplesPerPage = tuplesPerPage;
        this.m_pages = new ArrayList<DBPAXpage>();
	}

	@Override
	public void load() throws IOException {
		String rootPath = System.getProperty("user.dir");
        String filePath = rootPath + "/" + m_filename;
        BufferedReader reader;
        try {
        	// Control over the number of tuple per page saved
        	int pageNo = 0, tupleNo = 0;
        	this.m_pages.add(new DBPAXpage(this.m_schema, pageNo));
        	DBPAXpage currentPage = this.m_pages.get(pageNo);
        	
        	reader = new BufferedReader(new FileReader(filePath));
        	String line = reader.readLine();
        	
        	while(line != null)
        	{
        		
        		parseRowAndStore(line, currentPage);
        		line = reader.readLine();
        		++tupleNo;
        		
        		// Flip to next page when current page is full
        		if (tupleNo == this.m_tuplesPerPage) {
        			tupleNo = 0;
        			++pageNo;
                	this.m_pages.add(new DBPAXpage(this.m_schema, pageNo));
        			currentPage = this.m_pages.get(pageNo);
        		}
        	}
        	
        	// Add last empty page ???
//        	if (tupleNo != 0) {
//        		this.m_pages.add(new DBPAXpage(this.m_schema, pageNo + 1));
//            }
        	
        } catch (IOException e)
        {
        	e.printStackTrace();
        }
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// Get page number and relative tuple number in the corresponding page
		int pageNo = rownumber / this.m_tuplesPerPage;
		int tupleNo = rownumber % this.m_tuplesPerPage;
		
		try {
			return this.m_pages.get(pageNo).getTuple(tupleNo);
		} catch(IndexOutOfBoundsException e) {
			// Got to end of page so return EOF
//			System.err.printf("PAXStore::getRow: Index Out of bound; rownumber = %d\n", rownumber);
			return new DBTuple();
		}
	}
	
	private void parseRowAndStore(String line, DBPAXpage currentPage) {
		String[] inputs = line.split(m_delimiter);
		
		for(int i = 0; i < m_schema.length; ++i)
		{
			switch (m_schema[i]) {
			case INT:
				currentPage.addValue(Integer.parseInt(inputs[i]), i);
				break;
			case BOOLEAN:
				currentPage.addValue(Boolean.parseBoolean(inputs[i]), i);
				break;
			case DOUBLE:
				currentPage.addValue(Double.parseDouble(inputs[i]), i);
				break;
			case STRING:
				currentPage.addValue(inputs[i], i);
				break;
			}
		}
	}
}
