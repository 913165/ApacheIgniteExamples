package test.ignite.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlJdbcCopyExampleFromCSV2 {

	static Logger logger = LoggerFactory.getLogger(SqlJdbcCopyExampleFromCSV2.class);
    static int count = 0;
	public static void main(String[] args) throws Exception {
		logger.info("main method starts....");
		try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
			logger.info("Connected to server.");
			// Create table.
			// long startTime = System.currentTimeMillis();
			// long endTime = System.currentTimeMillis();
			// long duration = (endTime - startTime); //divide by 1000000 to get
			// milliseconds.
			// logger.info("time taken to insert records : "+duration);
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM stockdata")) {
					rs.next();
					logger.info("total count in stockdata table: " + rs.getLong(1) + "");
				}
			}

			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM stock_ref")) {
					rs.next();
					logger.info("total count in stock_ref table: " + rs.getLong(1) + "");
				}
			}
			
			List<String> isinList = new ArrayList<String>();
			logger.info("starting select query on isin data.");
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt
						.executeQuery("SELECT ISIN FROM stockdata order by TRADE_DATE desc limit 3000")) {
					while (rs.next()) {
						String isin = rs.getString(1);
						isinList.add(isin);
						// logger.info("ISIN: " + isin);
					}
				}
			}
			
			long startTime = System.currentTimeMillis();
			//logger.info("isinList: " + isinList);
			for (String strisin : isinList)
				try (Statement stmt = conn.createStatement()) {
					try (ResultSet rs = stmt
							.executeQuery("SELECT INTL_TICKER, SYMBOL from stock_ref a join stockdata b on a.INTL_TICKER = b.isin where isin = '"+strisin+"'")) {
						rs.next();
						String firstColumn = rs.getString(1);
						//logger.info("count "+count++);
						//logger.info("First element : " + firstColumn + " Second element : " + firstColumn);
					}
				}
			long endTime = System.currentTimeMillis();
			long duration = (endTime - startTime); //divide by 1000000 to get
			logger.info("time taken to get records : "+duration + " milliseonds..");
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("main method ends....");
	}
}