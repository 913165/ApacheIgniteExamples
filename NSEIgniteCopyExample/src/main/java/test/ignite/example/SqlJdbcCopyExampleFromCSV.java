package test.ignite.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlJdbcCopyExampleFromCSV {
	
	static Logger logger = LoggerFactory.getLogger(SqlJdbcCopyExampleFromCSV.class);
	public static void main(String[] args) throws Exception {
		logger.info("main method started...");
		Ignition.start("example-ignite.xml");
		// Open JDBC connection
		try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
			logger.info("Connected to server.");
			// Create table.
			executeCommand(conn, "DROP TABLE IF EXISTS Salaries");
			executeCommand(conn, "CREATE TABLE Salaries (" + "    ID CHAR(200), " + "    EmployeeName CHAR(200), "
					+ "    JobTitle CHAR(200), " + "    BasePay CHAR(200), " + "    OvertimePay CHAR(200), "
					+ "    PRIMARY KEY (ID) " + ") WITH \"template=partitioned, backups=1, CACHE_NAME=Salaries\"");
			logger.info("Created database objects.");
			// Load data from CSV file.
			executeCommand(conn, "COPY FROM '" + IgniteUtils.resolveIgnitePath("Salaries.csv") + "' "
					+ "INTO Salaries (ID, EmployeeName,JobTitle, BasePay, OvertimePay) FORMAT CSV");
			// Read data.
			try (Statement stmt = conn.createStatement()) {
				try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Salaries")) {
					rs.next();
					logger.info("Populated Salary table: " + rs.getLong(1) + " entries");
				}
			}
			// Drop database objects.
			try (Statement stmt = conn.createStatement()) {
				stmt.executeUpdate("DROP TABLE Salaries");
			}
			logger.info("Dropped database objects.");
		}
	}

	private static void executeCommand(Connection conn, String sql) throws Exception {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeUpdate(sql);
		}
	}
}