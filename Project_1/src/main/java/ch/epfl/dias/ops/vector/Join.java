package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator m_leftChild;
	private VectorOperator m_rightChild;
	private int m_leftFieldNo;
	private int m_rightFieldNo;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.m_leftChild = leftChild;
		this.m_rightChild = rightChild;
		this.m_leftFieldNo = leftFieldNo;
		this.m_rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}

}
