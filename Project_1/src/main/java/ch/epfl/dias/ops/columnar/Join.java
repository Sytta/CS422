package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	private ColumnarOperator m_leftChild;
	private ColumnarOperator m_rightChild;
	private int m_leftFieldNo;
	private int m_rightFieldNo;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.m_leftChild = leftChild;
		this.m_rightChild = rightChild;
		this.m_leftFieldNo = leftFieldNo;
		this.m_rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] leftColumns = this.m_leftChild.execute();
		DBColumn[] rightColumns = this.m_rightChild.execute();
		
		return null;
	}
}
