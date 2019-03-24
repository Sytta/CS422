package ch.epfl.dias.ops.columnar;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.*;

import org.junit.Before;
import org.junit.Test;

public class Task3ColumnarColumn {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    ColumnStore ColumnStoreOrder;
    ColumnStore ColumnStoreLineItem;
    
    @Before
    public void init()  {
        
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        ColumnStoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
        try {
			ColumnStoreOrder.load();
		} catch (IOException e) {
			// TODO Auto-generated catch columnar
			e.printStackTrace();
		}
        
        ColumnStoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        try {
			ColumnStoreLineItem.load();
		} catch (IOException e) {
			// TODO Auto-generated catch columnar
			e.printStackTrace();
		}        
    }
    
    
    @Test
    public void query1(){
        /* SELECT L.L_PARTKEY, L.L_LINESTATUS, L_RECEIPTDATE 
         * FROM lineitem L 
         * WHERE L.L_QUANTITY >= 20 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GE, 4, 20);
        ch.epfl.dias.ops.columnar.Project proj = new ch.epfl.dias.ops.columnar.Project(sel, new int[]{1,9,10});
            
        // This query should return only one result
        DBColumn[] result = proj.execute();
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
        
//        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        
        Integer[] output = result[0].getAsInteger();
        for (Object val : output) {
            System.out.println(val);
        }
//        assertTrue(output == 1284483);
    }
    
    @Test
    public void query2(){
        /* SELECT L.L_TAX, O.O_COMMENT
		 * FROM orders O, lineitem L
		 * WHERE O.O_ORDERKEY = L.L_ORDERKEY
		 * AND O.O_SHIPPRIORITY = 0 */	  
    	
    	long startTime = new Date().getTime();
    	
    	ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(ColumnStoreLineItem);
        
        /*Filtering */
        ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scanOrder, BinaryOp.EQ, 7, 0);
        
        ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanLineitem ,selOrder, 0, 0);
        
        ch.epfl.dias.ops.columnar.Project proj = new ch.epfl.dias.ops.columnar.Project(join, new int[]{7,24}); // L.L_TAX (7), O.O_COMMENT(24)
            
        // This query should return only one result
        DBColumn[] result = proj.execute();
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
        
        Double[] output = result[0].getAsDouble();
        for (Object val : output) {
            System.out.println(val);
        }
        
//        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
//        System.out.println(output);
//        assertTrue(output == 17971);
    }
    
    @Test
    public void query3(){
        /* SELECT MIN(L.L_DISCOUNT)
		 * FROM lineitem L
		 * WHERE L.L_QUANTITY < 30 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.columnar.Select selOrder = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.columnar.Project proj = new ch.epfl.dias.ops.columnar.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
        
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
//        assertTrue(output == 0.04);
    }
}