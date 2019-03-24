package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator m_leftChild;
	private VectorOperator m_rightChild;
	private int m_leftFieldNo;
	private int m_rightFieldNo;
	
	private int m_vectorsize;
	private int m_currentRightRowIndex;
	private DBColumn[] m_currentRightVector;
	private DBColumn[] m_eofColumns;
	
	private Hashtable<Object, ArrayList<Integer>> m_leftHashTable;
	private DBColumn[] m_leftCompleteColumns;
	private ArrayList<Integer> m_stitchedLeftIndexes;
	private ArrayList<Integer> m_stitchedRightIndexes;
	
	private DataType[] m_datatypes;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.m_leftChild = leftChild;
		this.m_rightChild = rightChild;
		this.m_leftFieldNo = leftFieldNo;
		this.m_rightFieldNo = rightFieldNo;
		this.m_vectorsize = leftChild.getVectorSize();
		
		this.m_leftHashTable = new Hashtable<Object, ArrayList<Integer>>();
		this.m_currentRightRowIndex = 0;
		
		this.m_stitchedLeftIndexes = new ArrayList<Integer>();
		this.m_stitchedRightIndexes= new ArrayList<Integer>();

	}

	@Override
	public void open() {
		this.m_leftChild.open();
		this.m_rightChild.open();
		
		DBColumn[] currentLeftVector = this.m_leftChild.next();
		
		this.m_leftCompleteColumns = new DBColumn[currentLeftVector.length];
		
		for (int i = 0; i < currentLeftVector.length; ++i) {
			this.m_leftCompleteColumns[i] = new DBColumn(currentLeftVector[i].getType());
		}
		
		while(!currentLeftVector[0].isEOF()) {
			// Load complete left columns in memory for further joining with right columns
			addToLeftCompleteColumns(currentLeftVector);
			currentLeftVector = this.m_leftChild.next();
		}
		
		// Construct hashtable on complete left columns
		buildHashTable();
		
		this.m_currentRightVector = this.m_rightChild.next();
		
		// Prepare eof columns
		this.m_eofColumns = new DBColumn[this.m_leftCompleteColumns.length + this.m_currentRightVector.length];
		for(int i = 0; i < this.m_eofColumns.length; ++i) {
			this.m_eofColumns[i] = new DBColumn();
		}
		
		// Save datatype for constructing result columns
		this.m_datatypes = new DataType[this.m_leftCompleteColumns.length + this.m_currentRightVector.length];
		for (int i = 0; i < this.m_leftCompleteColumns.length; ++i) {
			this.m_datatypes[i] = this.m_leftCompleteColumns[i].getType();
		}
		
		for (int i = this.m_leftCompleteColumns.length; i < this.m_datatypes.length; ++i) {
			this.m_datatypes[i] = this.m_currentRightVector[i - this.m_leftCompleteColumns.length].getType();
		}
	}

	@Override
	public DBColumn[] next() {
		
		// Construct result vector
		DBColumn[] result = new DBColumn[this.m_leftCompleteColumns.length + this.m_currentRightVector.length];
		for (int i = 0; i < result.length; ++i) {
			result[i] = new DBColumn(this.m_datatypes[i]);
		}
		
		// Stitches
		if (!this.m_stitchedLeftIndexes.isEmpty()) {
			mergeIndexes(result);
		}
		
		while((!this.m_currentRightVector[0].isEOF()) && (result[0].getLength() < this.m_vectorsize)) {
			
			if (this.m_currentRightRowIndex >= this.m_currentRightVector[0].getLength()) {
				this.m_currentRightVector = this.m_rightChild.next();
				
				if (this.m_currentRightVector[0].isEOF()) {
					// first vector returned is eof -> return eof
					if (result[0].getLength() == 0) {
						return this.m_eofColumns;
					} else {
						return result;
					}
				}
				
				this.m_currentRightRowIndex = 0;
			}
		
			Object[] rightValues = this.m_currentRightVector[this.m_rightFieldNo].getAsObject();
			
			computeJoinIndexes(rightValues, m_stitchedLeftIndexes, m_stitchedRightIndexes);
						
			mergeIndexes(result);
			
			// Last vector block %vectorsize < 3
			if (this.m_currentRightVector[0].getLength() < this.m_vectorsize) {
				break;
			}
			
		}
		
		if (result[0].getLength() == 0) {
			return this.m_eofColumns;
		} else {
			return result;
		}
		
	}

	@Override
	public void close() {
		this.m_leftChild.close();
		this.m_rightChild.close();
	}
	
	@Override
	public int getVectorSize() {
		return this.m_vectorsize;
	}
	
	private void addToLeftCompleteColumns(DBColumn[] leftVector) {
		for (int i = 0; i < leftVector.length; ++i) {
			this.m_leftCompleteColumns[i].addValues(leftVector[i]);
		}
	}
	
	private void buildHashTable() {
		
		Object[] values = this.m_leftCompleteColumns[this.m_leftFieldNo].getAsObject();
		
		int index = 0;
		for (Object value : values) {
			if (this.m_leftHashTable.containsKey(value)) {
				this.m_leftHashTable.get(value).add(index);
				
			} else {
				ArrayList<Integer> newIndexList = new ArrayList<Integer>();
				newIndexList.add(index);
				this.m_leftHashTable.put(value, newIndexList);
			}
			
			++index;
		}
	}
	
	private void computeJoinIndexes(Object[] rightValues, ArrayList<Integer> stitchedLeftIndexes, ArrayList<Integer> stitchedRightIndexes) {
		
		for(int i = this.m_currentRightRowIndex; i < rightValues.length; ++i, ++this.m_currentRightRowIndex) {
			
			if (this.m_leftHashTable.containsKey(rightValues[i])) {
				// Left indexes
				ArrayList<Integer> leftIndexes = this.m_leftHashTable.get(rightValues[i]);
				stitchedLeftIndexes.addAll(leftIndexes);
				
				// Right indexes
				for(int j = 0; j < leftIndexes.size(); ++j) {
					stitchedRightIndexes.add(i);
				}
			}
			
			// Already have more than 3 rows joined
			if (this.m_stitchedRightIndexes.size() >= this.m_vectorsize) {
				++this.m_currentRightRowIndex;
				break;
			}
		}
	}
	
	private DBColumn[] selectRows(DBColumn[] childColumns, ArrayList<Integer> selectedRowIndex) {
		
		DBColumn[] filteredColumns = new DBColumn[childColumns.length];
		
		filteredColumns[0] = childColumns[0].selectRows(selectedRowIndex);
		
		for(int i = 0; i < childColumns.length; ++i) {
			filteredColumns[i] = childColumns[i].selectRows(selectedRowIndex);
		}
				
		return filteredColumns;
	}
	
	private void mergeIndexes(DBColumn[] result) {
		// only choose 3 from each - i.e. pop up & delete first 3 indexes from each
		ArrayList<Integer> leftIndexes = new ArrayList<Integer>();
		ArrayList<Integer> rightIndexes = new ArrayList<Integer>();
		
		int totalSize = Math.min(this.m_vectorsize, this.m_stitchedLeftIndexes.size());
		
		for (int i = 0; i < totalSize; ++i) {
			leftIndexes.add(this.m_stitchedLeftIndexes.remove(0));
			rightIndexes.add(this.m_stitchedRightIndexes.remove(0));
		}
		
		// Merge results
		DBColumn[] leftResultingColumns = selectRows(this.m_leftCompleteColumns, leftIndexes);
		DBColumn[] rightResultingColumns = selectRows(this.m_currentRightVector, rightIndexes);
		
		System.arraycopy(leftResultingColumns, 0, result, 0, leftResultingColumns.length);
	    System.arraycopy(rightResultingColumns, 0, result, leftResultingColumns.length, rightResultingColumns.length);
	   
	}

}
