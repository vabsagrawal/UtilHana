package populatedata;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;


public class PopulateItemCur {
	
	String fileName = "item_list.csv";
	Integer startingFamId = 108188054;
	
	String insertQuery = "INSERT INTO US_WM_TABLES.ITEM_CUR(MDS_FAM_ID, ITEM_NBR, ITEM1_DESC, UPC_NBR, BASE_DIV_NBR, COUNTRY_CODE, COUNTRY_NAME, DEPT_NBR, DEPT_DESC, MDSE_CATG_NBR, MDSE_CATG_DESC, MDSE_SUBCATG_NBR, MDSE_SUBCATG_DESC, SUBCLASS_NBR, SUBCLASS_DESC, FINELINE_NBR, FINELINE_DESC, ACCTG_DEPT_NBR, ACCTG_DEPT_DESC, DEPT_SUBCATG_NBR, DEPT_SUBCATG_DESC, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, BUYER_RPT_POSTN_ID, PLANNER_RPT_POSTN_ID, VENDOR_NBR, VENDOR_NAME, VENDOR_DEPT_NBR, VENDOR_SEQ_NBR, OBSOLETE_DATE, GP_SUPPLIER_ID, GP_SUPPLIER_NAME, BASE_UNIT_RETAIL_AMT, CUST_BASE_RETAIL_AMT, BASE_RETAIL_UOM_CODE, BASE_RETAIL_UOM_DESC, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, VENDOR_STOCK_ID, CONSUMER_ITEM_NBR, CONSUMER_ITEM_DESC, PRODUCT_NBR, PRODUCT_DESC, BRAND_ID, BRAND_NAME, BRAND_OWNER_ID, BRAND_OWNER_NAME, BRAND_FAMILY_ID, BRAND_FAMILY_NAME, BUYING_REGION_CODE, BUYING_REGION_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC, ITEM_STATUS_CHANGE_DATE, ITEM_CREATE_DATE, ITEM_TYPE_CODE, ITEM_TYPE_DESC, ASSORTMENT_TYPE_CODE, ASSORTMENT_TYPE_DESC, MODULAR_BASED_MDSE_CODE, MODULAR_BASED_MDSE_DESC, REPL_SUBTYPE_CODE, REPL_SUBTYPE_DESC, SEASON_CODE, SEASON_DESC, SEASON_YEAR, WHSE_ALIGN_TYPE_CODE, WHSE_ALIGN_TYPE_DESC, WHSE_AREA_CODE, WHSE_AREA_DESC, WHSE_ROTATION_CODE, WHSE_ROTATION_DESC, WHPK_CALC_METHOD_CODE, WHPK_CALC_METHOD_DESC, CANCEL_WHEN_OUT_IND, ITEM_IMPORT_IND, ITEM_REPLENISHABLE_IND, NEVER_OUT_IND, GUARANTEED_SALES_IND, MASTER_CARTON_IND, PRIME_XREF_ITEM_NBR, PRIME_XREF_MDS_FAM_ID, PRIME_LIA_ITEM_NBR, PRIME_LIA_MDS_FAM_ID, ITEM_LENGTH_QTY, ITEM_HEIGHT_QTY, ITEM_WIDTH_QTY, ITEM_DIM_UOM_CODE, ITEM_DIM_UOM_DESC, ITEM_WEIGHT_QTY, ITEM_WEIGHT_UOM_CODE, ITEM_WEIGHT_UOM_DESC, ITEM_CUBE_QTY, ITEM_CUBE_UOM_CODE, ITEM_CUBE_UOM_DESC, ITEM_ORDER_EFFECTIVE_DATE, ITEM_EXPIRE_DATE, EST_OUT_OF_STOCK_DATE, WHSE_MIN_ORDER_QTY, WHSE_MAX_ORDER_QTY, SELL_QTY, SELL_UOM_CODE, SELL_UOM_DESC, SELL_PACKAGE_QTY, SELL_UNIT_QTY, SELL_UNIT_UOM_CODE, SELL_UNIT_UOM_DESC, PALLET_TI_QTY, PALLET_HI_QTY, PALLET_UPC_NBR, PALLET_SIZE_CODE, PALLET_ROUND_PCT, CASE_UPC_NBR, VNPK_COST_AMT, VNPK_QTY, VNPK_WEIGHT_QTY, VNPK_WEIGHT_UOM_CODE, VNPK_WEIGHT_UOM_DESC, VNPK_LENGTH_QTY, VNPK_WIDTH_QTY, VNPK_HEIGHT_QTY, VNPK_DIM_UOM_CODE, VNPK_DIM_UOM_DESC, VNPK_CUBE_QTY, VNPK_CUBE_UOM_CODE, VNPK_CUBE_UOM_DESC, VNPK_WEIGHT_FORMAT_CODE, VNPK_WEIGHT_FORMAT_DESC, VNPK_CSPK_CODE, VNPK_CSPK_DESC, VNPK_NETWGT_QTY, VNPK_NETWGT_UOM_CODE, WHPK_UPC_NBR, WHPK_SELL_AMT, WHPK_QTY, WHPK_LENGTH_QTY, WHPK_WIDTH_QTY, WHPK_CUBE_QTY, WHPK_CUBE_UOM_CODE, WHPK_CUBE_UOM_DESC, WHPK_HEIGHT_QTY, WHPK_DIM_UOM_CODE, WHPK_DIM_UOM_DESC, WHPK_WEIGHT_QTY, WHPK_WEIGHT_UOM_CODE, WHPK_WEIGHT_UOM_DESC, WHPK_UNIT_COST, ALL_LINKS_ITEM_NBR, ALL_LINKS_MDS_FAM_ID, STD_COUNTRY_CODE, COMP_PACKAGE_QTY, COMP_UNIT_QTY, COMP_UNIT_UOM_CODE, COMP_UNIT_UOM_DESC, ABC_PHARMA_NBR, ACCEPT_TEMP_HI_QTY, ACCEPT_TEMP_LO_QTY, ACCOUNT_NBR, ACCT_NBR_TYPE_CODE, ACCT_NBR_TYPE_DESC, ACTIVITY_CD, ACTIVITY_DESC, ALCOHOL_PCT, ALT_CHNNL_SRC_IND, BACKRM_SCALE_IND, BRAND_ACQUISITION_CODE, BRAND_ACQUISITION_DESC, CANNED_ORDER_IND, CASE_UPC_FORMAT_CD, CASE_UPC_FORMAT_DESC, CATCH_WEIGHT_IND, CNSUMABLE_DIV_NBR, CNTRL_SBSNC_IND, COMMODITY_ID, CONVEYABLE_IND, CRUSH_FACTOR_CODE, CRUSH_FACTOR_DESC, DC_DEA_REPORT_IND, DESTINATION_CODE, DESTINATION_DESC, DIET_TYPE_CODE, DIET_TYPE_DESC, DISCOUNT_IND, EXCLUS_SPLY_DC_NBR, FHS_DC_SLOT_CODE, FHS_DC_SLOT_DESC, FPP_BTCH_SHRNK_QTY, FPP_EST_SHRINK_PCT, FPP_MIN_VOLUME_QTY, FPP_PREPN_HR_QTY, FPP_RTRD_RANGE_IND, FPP_TRGT_THRWY_PCT, FPP_VOLUME_QTY, FPP_VOLUME_UOM_CD, FSA_IND, GIFT_CARD_FACE_AMT, GIFT_CARD_FEE_AMT, GIFT_CARD_FEE_PCT, GIFT_CARD_TYPE_CODE, GIFT_CARD_TYPE_DESC, IDEAL_TEMP_HI_QTY, IDEAL_TEMP_LO_QTY, INFRM_REORD_TYP_CD, INFRM_REORD_TYP_DESC, ITEM_SCANNABLE_IND, ITEM_SYNC_STAT_CD, ITEM_SYNC_STAT_DESC, ITEM_VOLUME_QTY, ITEM_VOLUME_UOM_CD, ITEMFILE_SOURCE_NM, LAST_ORDER_DATE, LICENSE_CODE, LICENSE_DESC, MARSHAL_ID, MDSE_PROGRAM_ID, MFGR_PRE_PRICE_AMT, MFGR_SUGD_RTL_AMT, MIN_PRICE_IND, MIN_RCVNG_DAYS_QTY, MIN_WHSE_LIFE_QTY, NON_MBR_UPCHRG_IND, ORD_SIZNG_FCTR_QTY, OSA_SUPPLIER_ID, PALLET_UPC_FMT_CD, PALLET_UPC_FMT_DESC, PERFORM_RATING_CD, PERFORM_RATING_DESC, PRESN_UNIT_QTY, PRICE_COMP_QTY, PRICE_COMP_UOM_CD, PRIME_UPC_ITEM_NBR, PRIME_UPC_MDS_FAM_ID, PROJ_YR_SALE_QTY, PROMO_ORD_BOOK_CD, PROMO_ORD_BOOK_DESC, PROMPT_PRICE_IND, QUALITY_CNTRL_CD, QUALITY_CNTRL_DESC, REPL_GROUP_NBR, REPLENISH_UNIT_IND, RESERVE_MDSE_CODE, RESERVE_MDSE_DESC, RESERVE_MDSE_IND, RETAIL_INCLUDE_VAT_IND, RETURN_RESALE_IND, RFID_IND, RPPC_IND, RTL_NOTFY_STR_IND, SANIT_REGTN_EXP_DT, SANITARY_REGTN_NBR, SECURITY_TAG_IND, SEGREGATION_CODE, SEGREGATION_DESC, SEND_STORE_DATE, SHELF_LBL_RQMT_IND, SHELF_ROTATION_IND, SHLF_LIFE_DAYS_QTY, STORE_FORMAT_CODE, STORE_FORMAT_DESC, STR_SHLF_LF_HR_QTY, SUPP_DISPLAY_IND, TEMP_SENSITIVE_IND, TEMPR_UOM_CODE, UPC_FORMAT_CODE, UPC_FORMAT_DESC, VARIABLE_COMP_IND, VARIABLE_WT_IND, VNDR_FIRST_ORD_DT, VNDR_FIRST_SHIP_DT, VNDR_FRST_AVAIL_DT, VNDR_INCRM_ORD_QTY, VNDR_LAST_SHIP_DT, VNDR_LEAD_TM_QTY, VNDR_MIN_ORD_QTY, VNDR_MINORD_UOM_CD, WHPK_NETWGT_QTY, WHPK_UPC_FORMAT_CD, WHPK_UPC_FORMAT_DESC)	VALUES(?, ?, 'GENTS 3 DIA HRZTL.03', 2040900884, 1, 'US', 'United States', 32, 'JEWELRY AND SUNGLASSES', 0, '*Unassigned', 78, 'UNASSIGNED.', 0, NULL, 3695, NULL, 32, 'JEWELRY AND SUNGLASSES', NULL, NULL, NULL, NULL, NULL, NULL, 31, 'APPAREL', 3, 'GENERAL MERCHANDISE', NULL, NULL, 740696, NULL, 32, 0, TO_DATE('1989-06-30','YYYY-MM-DD'), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3230869, 1290360, 'US', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";	
	
	public Connection getConnection() throws SQLException {
		Connection connection = null;                 
	    connection = DriverManager.getConnection("jdbc:sap://hpclb.no-thing.com:30415","USER","Password");
	    return connection;
	}
	
	public Connection getProdConnection() throws SQLException {
		Connection connection = null;                 
	    connection = DriverManager.getConnection("jdbc:sap://hpclb.no-thing.com:30415","USER","Password");
	    return connection;
	}
	
	public List<Integer> getItemNbrs() throws IOException {
		List<Integer> itemNbrs = new ArrayList();
		BufferedReader br = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)));
		String line;
		while ((line = br.readLine()) != null) {
            line = line.trim();
            itemNbrs.add(new Integer(line));
		}
		
		return itemNbrs;
	}
	
	public void insertIntoItemCur(Connection conn, List<Integer> itemNbrs) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(insertQuery);
		for (Integer item : itemNbrs) {		
			stmt.setInt(1, startingFamId++);
			stmt.setInt(2, item);
			stmt.executeUpdate();
			System.out.println("Inserted for item :" + item + " and famId : " + startingFamId);
		}
		System.out.println("Finished insertion with famID : " + (startingFamId-1));
	}
	
	public void insertProdDataIntoItemCur(Connection conn, Connection prodConn, List<Integer> itemNbrs) {
		String query = "select * from us_wm_tables.ITEM_CUR where ITEM_NBR=";
		for (Integer item : itemNbrs) {
			try {
				copy(query+Integer.toString(item),prodConn,conn);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public void copy(String query, Connection from, Connection to) throws SQLException {
		System.out.println(query);
	    try (PreparedStatement s1 = from.prepareStatement(query);
	         ResultSet rs = s1.executeQuery()) {
	        ResultSetMetaData meta = rs.getMetaData();

	        List<String> columns = new ArrayList<>();
	        for (int i = 1; i <= meta.getColumnCount(); i++)
	            columns.add(meta.getColumnName(i));

	        String outQuery = "INSERT INTO USER.ITEM_CUR ("
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
	
	public static void main(String[] argv) throws IOException, SQLException {
		
		PopulateItemCur p = new PopulateItemCur();
		
		p.fileName = argv[0];
		p.startingFamId = Integer.parseInt(argv[1]);
		
		List<Integer> itemNbrs = p.getItemNbrs(); //Arrays.asList(1,2,3);//p.getItemNbrs();
		
		System.out.println(itemNbrs.size());
		
		Connection conn = null;
		Connection prodConn = null;
		
	    try {
	    	conn = p.getConnection();
	    	//prodConn = p.getProdConnection();
	    } catch(SQLException e) {
	    	//System.err.println("Connection Failed");
	    }
	    p.insertIntoItemCur(conn, itemNbrs);
	    
	    //p.insertProdDataIntoItemCur(conn, prodConn, itemNbrs);
	}
}
