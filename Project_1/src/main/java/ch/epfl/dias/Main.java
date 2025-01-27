package ch.epfl.dias;

import java.io.IOException;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

public class Main {

	public static void main(String[] args) {

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
		try {
			rowstore.load();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		try {
			paxstore.load();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
		DBTuple currentTuple = scan.next();
//		while (!currentTuple.eof) {
		while (!currentTuple.isEOF()) {
			System.out.println(currentTuple.getFieldAsInt(1));
			currentTuple = scan.next();
		}

//		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
//		columnstoreData.load();
//		
//		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
//		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
//		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
//		DBColumn[] result = agg.execute();
//		int output = result[0].getAsInteger()[0];
//		System.out.println(output);

	}
}
