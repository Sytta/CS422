package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Date;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class Task3RowVolcano {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    RowStore RowStoreOrder;
    RowStore RowStoreLineItem;
    
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

        RowStoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
        try {
			RowStoreOrder.load();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        RowStoreLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        try {
			RowStoreLineItem.load();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");

    }
    
    
    @Test
    public void RowVolcanoquery1(){
        /* SELECT L.L_PARTKEY, L.L_LINESTATUS, L_RECEIPTDATE 
         * FROM lineitem L 
         * WHERE L.L_QUANTITY >= 20 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(RowStoreLineItem);
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 4, 20);
        ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(sel, new int[]{1,9,10});
            
        proj.open();
        
        // This query should return only one result
        DBTuple result = proj.next();
                
        while(!result.isEOF()) {
        	Integer output = result.getFieldAsInt(0);
//            System.out.println(output);
            result = proj.next();
        }
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");

    }
    
    @Test
    public void RowVolcanoquery2(){
        /* SELECT L.L_TAX, O.O_COMMENT
		 * FROM orders O, lineitem L
		 * WHERE O.O_ORDERKEY = L.L_ORDERKEY
		 * AND O.O_SHIPPRIORITY = 0 */	  
    	
    	long startTime = new Date().getTime();
    	
    	ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(RowStoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(RowStoreLineItem);
        
        /*Filtering */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.EQ, 7, 0);
        
        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLineitem ,selOrder, 0, 0);
        
        ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(join, new int[]{7,24}); // L.L_TAX (7), O.O_COMMENT(24)
            
        proj.open();
        
        // This query should return only one result
        DBTuple result = proj.next();
                
        while(!result.isEOF()) {
        	Double output = result.getFieldAsDouble(0);
//            System.out.println(output);
            result = proj.next();
        }
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");

    }
    
    @Test
    public void RowVolcanoquery3(){
        /* SELECT MIN(L.L_DISCOUNT)
		 * FROM lineitem L
		 * WHERE L.L_QUANTITY < 30 */
    	
    	long startTime = new Date().getTime();
    	
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(RowStoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new ch.epfl.dias.ops.volcano.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
        
        agg.open();
                
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
//        System.out.println(output + "\n");
        
        System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName() + " Duration = " + (new Date().getTime() - startTime) + "ms\r\n");

//        assertTrue(output == 0.04);
    }
}