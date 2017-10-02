package populatedata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PopulateInforemSku {
	
	String fileName = "";
	Integer sohLowerRange = 10;
	Integer sohUpperRange = 100;
	Integer startFamId =  108188054;
	Integer endFamId = 108188053 + 21;
	Integer sohLowerLimit = 10;
	Integer sohUpperLimit = 100;
	
	String insertQuery = "INSERT INTO US_WM_TABLES.INFOREM_MANAGED_SKU  (DIVISION_NBR, STORE_NBR, ITEM_NBR, ON_HAND_1_QTY, ON_ORDER_QTY, IN_TRANSIT_QTY, IN_WHS_QTY, DAILY_SALES_1_QTY, DAILY_SALES_2_QTY, DAILY_SALES_3_QTY, DAILY_SALES_4_QTY, DAILY_SALES_5_QTY, DAILY_SALES_6_QTY, DAILY_SALES_7_QTY, DAILY_SALES_8_QTY, WEEKLY_SALES_1_QTY, WEEKLY_SALES_2_QTY, WEEKLY_SALES_3_QTY, WEEKLY_SALES_4_QTY, WEEKLY_SALES_5_QTY, LAST_POS_APPLIED_DATE, LAST_RECEIVINGS_APPLIED_DATE, LAST_PROCESS_DATE, CARRY_OPTION, CARRIED_STATUS, TAB_IN_TRANSIT_QTY, TAB_IN_WHS_QTY, TAB_RECEIVING_QTY, TAB_ON_ORDER_QTY, TAB_PO_SHIP_DATE, BUYER_PUSH_ON_ORDER_QTY, BUYER_PUSH_REPLEN_ON_ORDER_QTY, BUYER_PUSH_IN_TRANSIT_QTY, BUYER_PUSH_IN_WHS_QTY, BUYER_PUSH_SHIP_DATE, LAST_SHIP_DATE, LAST_ON_HAND_COUNT_DATE, LAST_WTD_SALES_QTY, MAX_SALE_FLOOR_QTY, TSS_QTY, REPORT_EXCLUDE_IND, ON_HAND_2_QTY, ON_HAND_3_QTY, ON_HAND_4_QTY, ON_HAND_5_QTY, ON_HAND_6_QTY, ON_HAND_7_QTY, ON_HAND_8_QTY, ON_HAND_9_QTY, ON_HAND_10_QTY, ON_HAND_11_QTY, ON_HAND_12_QTY, ON_HAND_13_QTY, ON_HAND_14_QTY, ON_HAND_15_QTY, ON_HAND_16_QTY, ON_HAND_17_QTY, ON_HAND_18_QTY, ON_HAND_19_QTY, ON_HAND_20_QTY, ON_HAND_21_QTY, ON_HAND_22_QTY, ON_HAND_23_QTY, ON_HAND_24_QTY, ON_HAND_25_QTY, ON_HAND_26_QTY, ON_HAND_27_QTY, ON_HAND_28_QTY, ON_HAND_29_QTY, ON_HAND_30_QTY, ON_HAND_31_QTY, ON_HAND_32_QTY, ON_HAND_33_QTY, ON_HAND_34_QTY, ON_HAND_35_QTY) VALUES(1, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, TO_DATE('1950-01-01','YYYY-MM-DD'), TO_DATE('1950-01-01','YYYY-MM-DD'), TO_DATE('2016-12-20','YYYY-MM-DD'), 'R', 'S', 0, 0, 0, 0, TO_DATE('1950-01-01','YYYY-MM-DD'), 0, 0, 0, 0, TO_DATE('1950-01-01','YYYY-MM-DD'), TO_DATE('1950-01-01','YYYY-MM-DD'), TO_DATE('1950-01-01','YYYY-MM-DD'), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";
	
	public Connection getConnection() throws SQLException {
		Connection connection = null;                 
	    connection = DriverManager.getConnection("jdbc:sap://shdbdeva100.no-thing.com:31515","USER","Password");
	    return connection;
	}
	
	public Connection getProdConnection() throws SQLException {
		Connection connection = null;                 
	    connection = DriverManager.getConnection("jdbc:sap://hpclb.no-thing.com:30415","USER","password");
	    return connection;
	}
	
	public List<Integer> getStoreNbrs() throws NumberFormatException, IOException {
		List<Integer> stores = new ArrayList<>();
		BufferedReader br = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)));
		String line;
		while ((line = br.readLine()) != null) {
            String[] fields = line.trim().split(",");
            stores.add(new Integer(fields[0]));
		}
		return stores;
	}
	
	public Integer generateRandomSoh(Integer Min, Integer Max) {
		return Min + (int)(Math.random() * ((Max - Min) + 1));	
	}
	
	public void insertIntoInforem(Connection conn, List<Integer> stores) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(insertQuery);
		for (Integer famId=startFamId; famId<=endFamId ; famId++) {
			for (Integer store : stores) {
				stmt.setInt(1, store);
				stmt.setInt(2, famId);
				stmt.setInt(3, generateRandomSoh(sohLowerLimit, sohUpperLimit));
				stmt.executeUpdate();
				//System.out.println("Inserted for store " + store);
			}
			System.out.println("Inserted for famId " + famId);
		}
	}
	
	public void insertProdDataIntoInforem(Connection conn, Connection prodConn, List<Integer> itemNbrs) {
		String query = "select MDS_FAM_ID from us_wm_tables.ITEM_CUR where ITEM_NBR=";
		String query2 = "select * from us_wm_tables.INFOREM_MANAGED_SKU where ITEM_NBR=";
		for (Integer item : itemNbrs) {
			try {
				Integer famId = getFamId(query+Integer.toString(item), prodConn);
				copy(query2+Integer.toString(famId),prodConn,conn);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	private Integer getFamId(String query, Connection conn) throws SQLException {
		System.out.println(query);
		PreparedStatement stmt = conn.prepareStatement(query);
		ResultSet rs  = stmt.executeQuery();
		if (rs.next()) {
			return rs.getInt(1);
		} else {
			return null;
		}
	}

	public void copy(String query, Connection from, Connection to) throws SQLException {
		System.out.println(query);
	    try (PreparedStatement s1 = from.prepareStatement(query);
	         ResultSet rs = s1.executeQuery()) {
	        ResultSetMetaData meta = rs.getMetaData();
	        System.out.println(meta.getColumnCount());
	        List<String> columns = new ArrayList<>();
	        for (int i = 1; i <= meta.getColumnCount(); i++)
	            columns.add(meta.getColumnName(i));

	        String outQuery = "INSERT INTO USER.INFOREM_MANAGED_SKU ("
		              + columns.stream().collect(Collectors.joining(", "))
		              + ") VALUES ("
		              + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
		              + ")";
	        System.out.println(outQuery);
	        try (PreparedStatement s2 = to.prepareStatement(outQuery)) {

	            while (rs.next()) {
	                for (int i = 1; i <= meta.getColumnCount(); i++)
	                    s2.setObject(i, rs.getObject(i));

	                s2.addBatch();
	            }

	            s2.executeBatch();
	        }
	    }
	}
	
	public static void print(List<Object[]> objects) {
		for (Object[] o : objects) {
			for (Object i : o)
				System.out.println(i);
		}
	}
	
	public static void main(String[] argv) throws IOException, SQLException {
		PopulateInforemSku p = new PopulateInforemSku();

		p.fileName = argv[0];
		p.startFamId = Integer.parseInt(argv[1]);
		p.endFamId = Integer.parseInt(argv[2]);
		
		List<Integer> stores = p.getStoreNbrs();
		System.out.println(stores.size());
		//List<Integer> itemNbrs = Arrays.asList(1,2,3,4);//p.getItemNbrs();
		Connection conn = null;
		Connection prodConn = null;
		try {
	    	conn = p.getConnection();
	    	//prodConn = p.getProdConnection();
	    } catch(SQLException e) {
	    	System.err.println("Connection Failed");
	    }
		//p.insertProdDataIntoInforem(conn, prodConn, itemNbrs);
		p.insertIntoInforem(conn, stores);
	}
}
