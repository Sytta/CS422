package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	private VectorOperator m_child;
	private BinaryOp m_op;
	private int m_fieldNo;
	private int m_selectValue;
	
	private int m_vectorsize;

	private DBColumn[] m_resultVector;
	
	private int m_currentRowIndex;
	private DBColumn[] m_currentChildVector;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.m_child = child;
		this.m_op = op;
		this.m_fieldNo = fieldNo;
		this.m_selectValue = value;
		this.m_currentRowIndex = -1;
		this.m_vectorsize = child.getVectorSize();
	}
	
	@Override
	public void open() {
		this.m_child.open();
		this.m_currentRowIndex = 0;
		this.m_currentChildVector = this.m_child.next();
		
		this.m_resultVector = new DBColumn[this.m_currentChildVector.length];
	}

	@Override
	public DBColumn[] next() {
		// Empty result vector
		this.initResultVector();
		
		while(this.m_resultVector[0].getLength() < this.m_vectorsize) {
			
			if (this.m_currentRowIndex >= this.m_currentChildVector[0].getLength()) {
				this.m_currentChildVector = this.m_child.next();
				
				if (this.m_currentChildVector[0].isEOF()) {
						// /vectorsize = 0, first db is already eof
						return this.m_currentChildVector;
				}
				
				this.m_currentRowIndex = 0;
			}
			
			
			DBColumn column = this.m_currentChildVector[this.m_fieldNo];
			performSelect(column);
			
			// Last vector block %vectorsize < 3
			if (this.m_currentChildVector[0].getLength() < this.m_vectorsize) {
				break;
			}

		}
		
		return this.m_resultVector;

	}
	
	@Override
	public void close() {
		this.m_child.close();
	}
	
	private void initResultVector() {
		
		for(int i = 0; i < this.m_resultVector.length; ++i) {
			DataType type = this.m_currentChildVector[i].getType();
			this.m_resultVector[i] = new DBColumn(type);
		}
		
	}
	
	@Override
	public int getVectorSize() {
		return this.m_vectorsize;
	}
	
	private void performSelect(DBColumn column) {
		
		Integer[] columnValues = column.getAsInteger();
		
		for (int i = this.m_currentRowIndex; i < columnValues.length; ++i, ++this.m_currentRowIndex) 
		{
			switch (this.m_op) {
			case EQ:
				if (columnValues[i] == this.m_selectValue)
					addFields(i);
				break;
			case GE:
				if (columnValues[i] >= this.m_selectValue)
					addFields(i);
				break;
			case GT:
				if (columnValues[i] > this.m_selectValue)
					addFields(i);
				break;
			case LE:
				if (columnValues[i] <= this.m_selectValue)
					addFields(i);
				break;
			case LT:
				if (columnValues[i] < this.m_selectValue)
					addFields(i);
				break;
			case NE:
				if (columnValues[i] != this.m_selectValue)
					addFields(i);
				break;
			}
			
			// if #value found attains vectorsize, stop and returns
			if (this.m_resultVector[0].getLength() == this.m_vectorsize) {
				++this.m_currentRowIndex; // increase row index for next turn
				return;
			}
		}
		
	}


	private void addFields(int index) {
		for (int i = 0; i < this.m_currentChildVector.length; ++i) {
			this.m_resultVector[i].addValue(this.m_currentChildVector[i].getValue(index));
		}
	}
}
