package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.column.DBColumnId;

import java.util.ArrayList;
import java.util.Hashtable;

public class Join implements ColumnarOperator {

	private ColumnarOperator m_leftChild;
	private ColumnarOperator m_rightChild;
	private int m_leftFieldNo;
	private int m_rightFieldNo;
	
	private Hashtable<Object, ArrayList<Integer>> m_leftHashTable;
	private ArrayList<Integer> m_stitchedLeftIndexes = new ArrayList<Integer>();
	private ArrayList<Integer> m_stitchedRightIndexes= new ArrayList<Integer>();

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.m_leftChild = leftChild;
		this.m_rightChild = rightChild;
		this.m_leftFieldNo = leftFieldNo;
		this.m_rightFieldNo = rightFieldNo;
		
		this.m_leftHashTable = new Hashtable<Object, ArrayList<Integer>>();
		this.m_stitchedLeftIndexes = new ArrayList<Integer>();
		this.m_stitchedRightIndexes= new ArrayList<Integer>();
	}

	public DBColumn[] execute() {
		DBColumn[] leftColumns = this.m_leftChild.execute();
		DBColumn[] rightColumns = this.m_rightChild.execute();
		
		Object[] leftValues = leftColumns[this.m_leftFieldNo].getAsObject();
		Object[] rightValues = rightColumns[this.m_rightFieldNo].getAsObject();
		
		buildHashTable(this.m_leftHashTable, leftValues);
		
		computeJoinIndexes(rightValues);
		
		// Merge results
		DBColumn[] leftResultingColumns = selectRows(leftColumns, this.m_stitchedLeftIndexes);
		DBColumn[] rightResultingColumns = selectRows(rightColumns, this.m_stitchedRightIndexes);
		
		DBColumn[] result = new DBColumn[leftColumns.length + rightColumns.length];
		System.arraycopy(leftResultingColumns, 0, result, 0, leftResultingColumns.length);
	    System.arraycopy(rightResultingColumns, 0, result, leftResultingColumns.length, rightResultingColumns.length);
		

		return result;
	}
	
	private void buildHashTable(Hashtable<Object, ArrayList<Integer>> hashTable, Object[] values) {
				
		int index = 0;
		for (Object value : values) {
			if (hashTable.containsKey(value)) {
				hashTable.get(value).add(index);
				
			} else {
				ArrayList<Integer> newIndexList = new ArrayList<Integer>();
				newIndexList.add(index);
				hashTable.put(value, newIndexList);
			}
			
			++index;
		}
	}
	
	private void computeJoinIndexes(Object[] rightValues) {
		
		for(int i = 0; i < rightValues.length; ++i) {
			
			if (this.m_leftHashTable.containsKey(rightValues[i])) {
				// Left indexes
				ArrayList<Integer> leftIndexes = this.m_leftHashTable.get(rightValues[i]);
				this.m_stitchedLeftIndexes.addAll(leftIndexes);
				
				// Right indexes
				for(int j = 0; j < leftIndexes.size(); ++j) {
					this.m_stitchedRightIndexes.add(i);
				}
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
	
		
}
