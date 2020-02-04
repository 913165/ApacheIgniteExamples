package test.ignite.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlJdbcCopyExampleFromCSV2 {

	static Logger logger = LoggerFactory.getLogger(SqlJdbcCopyExampleFromCSV2.class);

	public static void main(String[] args) throws Exception {
		logger.info("main method started...");
		Ignition.start("example-ignite.xml");
		// Open JDBC connection
		try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
			logger.info("Connected to server.");
			// Create table.
			executeCommand(conn, "DROP TABLE IF EXISTS STOCK_REF");
			executeCommand(conn, "DROP TABLE IF EXISTS stockdata");
			logger.info("before creating tables.");

			
			  executeCommand(conn,
			  "CREATE TABLE STOCK_REF ( INTL_TICKER VARCHAR(12),NSE_SYMBOL VARCHAR(10), " +
			  "PRIMARY KEY (INTL_TICKER) ) WITH \"template=partitioned, backups=1, CACHE_NAME=STOCK_REF\""
			  );
			 

			executeCommand(conn, "CREATE TABLE stockdata(\r\n"
					+ "					   SYMBOL      VARCHAR(10) NOT NULL \r\n"
					+ "					  ,SERIES      VARCHAR(2) \r\n"
					+ "					  ,OPEN        NUMERIC(8,2) \r\n"
					+ "					  ,HIGH        NUMERIC(8,2) \r\n"
					+ "					  ,LOW         NUMERIC(7,2) \r\n"
					+ "					  ,CLOSE       NUMERIC(7,2) \r\n"
					+ "					  ,LAST        NUMERIC(7,2) \r\n"
					+ "					  ,PREVCLOSE   NUMERIC(8,2) \r\n"
					+ "					  ,TOTTRDQTY   INTEGER  \r\n"
					+ "					  ,TOTTRDVAL   NUMERIC(11,2) \r\n"
					+ "					  ,TRADE_DATE  DATE  \r\n" + "					  ,TOTALTRADES INTEGER  \r\n"
					+ "					  ,ISIN        VARCHAR(12) \r\n"
					+ "			,PRIMARY KEY(SYMBOL,SERIES,TRADE_DATE)		) WITH \"template=partitioned, backups=1, CACHE_NAME=stockdata\"");
			logger.info("after creating table.");

			long startTime = System.currentTimeMillis();
			// Load data from CSV file.
			executeCommand(conn, "COPY FROM '" + IgniteUtils.resolveIgnitePath("nsestock.csv") + "' "
					+ " INTO stockdata(SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TRADE_DATE,TOTALTRADES,ISIN)  FORMAT CSV");
			// Read data.

			logger.info("DATA LOADED INTO STOCK_DATA");

			long endTime = System.currentTimeMillis();
			try {
				logger.info("creating index on stockdata(ISIN)");
				executeCommand(conn, " create index on stockdata(ISIN)");
				logger.info("index created on stockdata(ISIN)");
			} catch (Exception e) {
				e.printStackTrace();
			}

			long duration = (endTime - startTime); // divide by 1000000 to get milliseconds.
			logger.info("time taken to insert records : " + duration + " milliseconds");

			//INTL_TICKER VARCHAR(12),NSE_SYMBOL 
			
			logger.info("creating STOCK_REF table");
			executeCommand(conn, "insert into STOCK_REF (INTL_TICKER)  SELECT DISTINCT ISIN SYMBOL FROM stockdata ");
			logger.info("created STOCK_REF table");

			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM stockdata")) {
					rs.next();
					logger.info("Populated stockdata table: " + rs.getLong(1) + " entries");
				}
			}
			logger.info("server ruinning....");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void executeCommand(Connection conn, String sql) throws Exception {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(sql);
		}
	}
}