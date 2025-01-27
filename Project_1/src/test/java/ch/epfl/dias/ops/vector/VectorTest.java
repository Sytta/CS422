package ch.epfl.dias.ops.vector;

import java.io.IOException;
import java.util.Arrays;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.ops.volcano.ProjectAggregate;
import ch.epfl.dias.ops.volcano.Select;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

public class VectorTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;
	ColumnStore columnstoreEmpty;

	Double[] orderCol3 = { 55314.82, 66219.63, 270741.97, 41714.38, 122444.33, 50883.96, 287534.80, 129634.85,
			126998.88, 186600.18 };
	int numTuplesData = 11;
	int numTuplesOrder = 10;
	int standardVectorsize = 3;

	// 1 seconds max per method tested
	@Rule
	public Timeout globalTimeout = Timeout.seconds(1);

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

//		columnstoreEmpty = new ColumnStore(schema, "input/empty.csv", ",");
//		columnstoreEmpty.load();
	}
	
	@Test
	public void sTestData() {
		// Read all the rows
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		
		scan.open();
		DBColumn[] result = scan.next();
		String resultPrint = "";
		
		while (!result[0].isEOF()) {
			
			for (int j = 0; j < result[0].getLength(); ++j) {
				for (int i = 0; i < result.length; ++i) {
					DBColumn col = result[i];
					resultPrint += col.getValue(j).toString() + " // ";
				}
				
//				System.out.println(resultPrint);
				resultPrint = "";
			}
			
			result = scan.next();
		}
	}

	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
		
		DBColumn[] eofResult = agg.next();
		assertTrue(eofResult[0].isEOF());
	}
	
	@Test
    public void spTestData1(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
    
        // This query should return only one result
		sel.open();
        DBColumn[] returnedColumns = sel.next();
        
        Integer[] result = returnedColumns[0].getAsInteger();
       
        int[] realOutput = new int[] {1, 6, 8};
        
        for(int i = 0; i < realOutput.length; ++i) {
        	assertTrue(result[i] == realOutput[i]);
        }  
        
    }

    @Test
    public void spTestData2(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{0,2,4,5});
            
        // This query should return only one result
        proj.open();
        DBColumn[] returnedColumns = proj.next();
        
        Integer[] result = returnedColumns[3].getAsInteger(); // 5th col
        
        int[] realOutput = new int[] {6, 6, 6};
        
        for(int i = 0; i < realOutput.length; ++i) {
        	assertTrue(result[i] == realOutput[i]);
        }
        
        DBColumn[] eofColumns = proj.next();
        
        for (DBColumn col : eofColumns) {
        	DBColumn aa = col;
        	assertTrue(col.isEOF());
        }
    }

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.STRING, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel,
				Aggregate.COUNT, DataType.INT, 2);

		agg.open();
		DBColumn[] result = agg.next();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
	
	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
	
	    /*Filtering on both sides */
		ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ,0,3);
		ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ,0,3);
	
		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder,selLineitem,0,0);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
		agg.open();
	    //This query should return only one result
	    DBColumn[] result = agg.next();
	    
	    int output = result[0].getAsInteger()[0];
	    assertTrue(output == 3);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, standardVectorsize);
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, standardVectorsize);
	
	    /*Filtering on both sides */
		ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ,0,3);
		ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ,0,3);
	
		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selLineitem,selOrder,0,0);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBColumn result = agg.next()[0];
	    int output = result.getAsInteger()[0];
	    assertTrue(output == 3);
	}

}
