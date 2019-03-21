package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	private ColumnarOperator m_child;
	private int[] m_projectedColumns;

	public Project(ColumnarOperator child, int[] columns) {
		this.m_child = child;
		this.m_projectedColumns = columns;
	}

	public DBColumn[] execute() {
		DBColumn[] childColumns = this.m_child.execute();
		DBColumn[] results = new DBColumn[this.m_projectedColumns.length];
        int index = 0;
        for (int columnToGet : this.m_projectedColumns) {
            results[index++] = childColumns[columnToGet];
        }
        return results;
	}
}
