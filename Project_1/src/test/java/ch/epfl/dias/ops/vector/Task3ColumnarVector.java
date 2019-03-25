package ch.epfl.dias.ops.vector;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.*;

import org.junit.Before;
import org.junit.Test;

public class Task3ColumnarVector {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    ColumnStore ColumnStoreOrder;
    ColumnStore ColumnStoreLineItem;
    
	int standardVectorsize = 10000;
    
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
        
    	long startTime = new Date().getTime();
        
        ColumnStoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        try {
			ColumnStoreOrder.load();
		} catch (IOException e) {
			// TODO Auto-generated catch vector
			e.printStackTrace();
		}
        
        ColumnStoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        try {
			ColumnStoreLineItem.load();
		} catch (IOException e) {
			// TODO Auto-generated catch vector
			e.printStackTrace();
		}        
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
    }
    
    
    @Test
    public void ColumnarVectorquery1(){
        /* SELECT L.L_PARTKEY, L.L_LINESTATUS, L_RECEIPTDATE 
         * FROM lineitem L 
         * WHERE L.L_QUANTITY >= 20 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(ColumnStoreLineItem, standardVectorsize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, 4, 20);
        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{1,9,10});
            
        proj.open();
        
        // This query should return only one result
        DBColumn[] result = proj.next();
        
//        String output;
        while(!result[0].isEOF()){
        	Integer[] output = result[0].getAsInteger();
            for(Object val: output){
//                System.out.println(val);
            }
            result = proj.next();
        }
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
        
    }
    
    @Test
    public void ColumnarVectorquery2(){
        /* SELECT L.L_TAX, O.O_COMMENT
		 * FROM orders O, lineitem L
		 * WHERE O.O_ORDERKEY = L.L_ORDERKEY
		 * AND O.O_SHIPPRIORITY = 0 */	  
    	
    	long startTime = new Date().getTime();
    	
    	ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(ColumnStoreOrder, standardVectorsize);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(ColumnStoreLineItem, standardVectorsize);
        
        /*Filtering */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 7, 0);
        
        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scanLineitem ,selOrder, 0, 0);
        
        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(join, new int[]{7,24}); // L.L_TAX (7), O.O_COMMENT(24)
            
        proj.open();
        // This query should return only one result
        DBColumn[] result = proj.next();
                
        Double[] output;
        while(!result[0].isEOF()){
            output = result[0].getAsDouble();
//            int outputSingle = output[output.length-1];
            for(Object val: output){
//                System.out.println(val);
            }
            result = proj.next();
        }
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");
        
    }
    
    @Test
    public void ColumnarVectorquery3(){
        /* SELECT MIN(L.L_DISCOUNT)
		 * FROM lineitem L
		 * WHERE L.L_QUANTITY < 30 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(ColumnStoreLineItem, standardVectorsize);
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.vector.Project proj = new ch.epfl.dias.ops.vector.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
            
        agg.open();
     // This query should return only one result
        DBColumn[] result = agg.next();
        double output = result[0].getAsDouble()[0];
//        System.out.println(output + "\n");
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");

//        assertTrue(output == 0.04);
    }
}