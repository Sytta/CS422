package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	private ColumnarOperator m_child;
	private BinaryOp m_op;
	private int m_fieldNo;
	private int m_selectValue;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		this.m_child = child;
		this.m_op = op;
		this.m_fieldNo = fieldNo;
		this.m_selectValue = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] childColumns = m_child.execute();
		// Get selected row indexes
        ArrayList<Integer> selectedRowIndex = getSelectedRowIndex(childColumns[this.m_fieldNo]);
		
		return selectRows(childColumns, selectedRowIndex);
	}
	
	public ArrayList<Integer> getSelectedRowIndex(DBColumn column) {
		
		ArrayList<Integer> selectedRowIndex = new ArrayList<Integer>();
		int i = 0;
		for (Integer columnValue : column.getAsInteger()) {
			switch (this.m_op) {
			case EQ:
				if (columnValue == this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			case GE:
				if (columnValue >= this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			case GT:
				if (columnValue > this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			case LE:
				if (columnValue <= this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			case LT:
				if (columnValue < this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			case NE:
				if (columnValue != this.m_selectValue)
					selectedRowIndex.add(i);
				break;
			}
			
			++i;
		}
		
		return selectedRowIndex;
	}
	
	public DBColumn[] selectRows(DBColumn[] childColumns, ArrayList<Integer> selectedRowIndex) {
		
		DBColumn[] filteredColumns = new DBColumn[childColumns.length];
		
		// Filter per column
		for(int i = 0; i < childColumns.length; ++i) {
			filteredColumns[i] = childColumns[i].selectRows(selectedRowIndex);
		}
		
		Integer[] col = filteredColumns[0].getAsInteger();
		
		return filteredColumns;
	}
	
	
}
