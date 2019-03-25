package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class ColumnarTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;

	@Before
	public void init() throws IOException {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
		columnstoreLineItem.load();
	}
	
    @Test
    public void simpleTest(){
        DBColumn[] columns = columnstoreLineItem.getColumns(new int[]{10});
        DBColumn col = columns[0];
        String[] x = col.getAsString();
         for(String c: x){
//         	System.out.println(c);
         }
//         System.out.println(columns[0].type);
        assertEquals(x[3],"1996-04-21");
    }

	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
	
    @Test
    public void spTestData1(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
        ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
        ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
    
        // This query should return only one result
        DBColumn result = sel.execute()[0];
       
        Integer[] output = result.getAsInteger();
        int[] realOutput = new int[] {1, 6, 8};
        
        for(int i = 0; i < realOutput.length; ++i) {
        	assertTrue(output[i] == realOutput[i]);
        }
        
        
    }

    @Test
    public void spTestData2(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
        ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
        ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
        ch.epfl.dias.ops.columnar.Project proj = new ch.epfl.dias.ops.columnar.Project(sel, new int[]{0,2,4,5});
            
        // This query should return only one result
        DBColumn result = proj.execute()[3];
        
        Integer[] output = result.getAsInteger();
        int[] realOutput = new int[] {6, 6, 6};
        
        for(int i = 0; i < realOutput.length; ++i) {
        	assertTrue(output[i] == realOutput[i]);
        }
    }

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.STRING, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(selOrder, selLineitem, 0, 0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(columnstoreOrder);
		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.columnar.Select selLineitem = new ch.epfl.dias.ops.columnar.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(selLineitem, selOrder, 0, 0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
}