package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator m_leftChild;
	private VolcanoOperator m_rightChild;
	private int m_leftFieldNo;
	private int m_rightFieldNo;
	
	private Hashtable<Object, ArrayList<DBTuple>> m_hashTable;
	private Iterator<DBTuple> m_leftIterator;
	private ArrayList<DBTuple> m_leftMachingTuples;
	private DBTuple m_currentRightTuple;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.m_leftChild = leftChild;
		this.m_rightChild = rightChild;
		this.m_leftFieldNo = leftFieldNo;
		this.m_rightFieldNo = rightFieldNo;
		
		this.m_hashTable = new Hashtable<Object, ArrayList<DBTuple>>();
		this.m_leftMachingTuples = null;
		this.m_currentRightTuple = null;
	}

	@Override
	public void open() {
		this.m_leftChild.open();
		this.m_rightChild.open();
		this.buildHashTable();
		
		this.m_currentRightTuple = this.m_rightChild.next();
		Object currentRightField = this.m_currentRightTuple.getFieldAsObject(this.m_rightFieldNo);
		this.findNextCorrespondingLeftTupleList(currentRightField);

	}

	@Override
	public DBTuple next() {	
		while (!this.m_currentRightTuple.isEOF())
		{
			Object currentRightField = this.m_currentRightTuple.getFieldAsObject(this.m_rightFieldNo);

			if (this.m_leftIterator != null && m_leftIterator.hasNext()) {
				return this.joinTuples(m_leftIterator.next());
			} else {
				
				this.m_currentRightTuple = this.m_rightChild.next();
				
				if (!this.m_currentRightTuple.isEOF()) {
					
					currentRightField = this.m_currentRightTuple.getFieldAsObject(this.m_rightFieldNo);
					this.findNextCorrespondingLeftTupleList(currentRightField);
				}
			} 
		}
		
		// EOF
		return this.m_currentRightTuple;
		
	}

	@Override
	public void close() {
		this.m_leftChild.close();
		this.m_rightChild.close();
	}
	
	private void buildHashTable() {
		DBTuple currentLeftTuple = this.m_leftChild.next();
		Object currentLeftField;

		while(!currentLeftTuple.isEOF()) {
			currentLeftField = currentLeftTuple.getFieldAsObject(this.m_leftFieldNo);		

			if (this.m_hashTable.containsKey(currentLeftField)) {
				this.m_hashTable.get(currentLeftField).add(currentLeftTuple);
				
			} else {
				ArrayList<DBTuple> newTupleList = new ArrayList<DBTuple>();
				newTupleList.add(currentLeftTuple);
				this.m_hashTable.put(currentLeftTuple.getFieldAsObject(this.m_leftFieldNo), newTupleList);
			}
			
			currentLeftTuple = this.m_leftChild.next();
		}
	}
	
	private void findNextCorrespondingLeftTupleList(Object rightField) {
		// Reset left tuple list
		this.m_leftMachingTuples = null;
		
		if (this.m_hashTable.containsKey(rightField)) {
			this.m_leftMachingTuples = this.m_hashTable.get(rightField);
			this.m_leftIterator = this.m_leftMachingTuples.iterator();
		}
		
	}
	
	private DBTuple joinTuples(DBTuple leftTuple) {
		
		// Merge 2 tuples
		DataType[] leftDataType = leftTuple.getFieldTypes();
		DataType[] rightDataType = this.m_currentRightTuple.getFieldTypes();				
		int totalFieldLength = leftDataType.length + rightDataType.length;
		
		// Copy the dataTypes
		DataType[] mergedDataType = new DataType[totalFieldLength];
		System.arraycopy(leftDataType, 0, mergedDataType, 0, leftDataType.length);
	    System.arraycopy(rightDataType, 0, mergedDataType, leftDataType.length, rightDataType.length);
	    
	    // Copy the fields 
		Object[] mergedFields = new Object[totalFieldLength];
		System.arraycopy(leftTuple.getAllFields(), 0, mergedFields, 0, leftDataType.length);
	    System.arraycopy(this.m_currentRightTuple.getAllFields(), 0, mergedFields, leftDataType.length, rightDataType.length);
	    
	    return new DBTuple(mergedFields, mergedDataType);
		
	}
}
