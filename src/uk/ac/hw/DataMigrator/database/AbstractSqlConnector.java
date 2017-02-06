package uk.ac.hw.DataMigrator.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mysql.jdbc.StringUtils;

public abstract class AbstractSqlConnector implements Runnable, AutoCloseable {
	private Connection connection;
	private PreparedStatement statement;
	private PreparedStatement getUsersStatement;
	
	private int batchSize;
	private ConcurrentLinkedQueue<ResultSet> outputQueue;
	
	private static final String DB_URL = "";
	private static final String USER = "";
	private static final String SQL_STRING = "";
	
	public AbstractSqlConnector() throws Exception{
		this.connection = DriverManager.getConnection(DB_URL, USER, readPasswordFromArgs());
		this.statement = connection.prepareStatement(SQL_STRING);
	}
	
	public ResultSet getResultBatch(int startId, int endId) throws SQLException {
		statement.clearParameters();
		statement.setInt(1, startId);
		statement.setInt(2, endId);
		return statement.executeQuery();
	}
	
	private String readPasswordFromArgs() throws NullPointerException {
		String pass = System.getProperty("SQL_PASS");
		if(StringUtils.isNullOrEmpty(pass)) {
			 throw new NullPointerException("Could not read password");
		} 
		return pass;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		if(statement != null && !statement.isClosed()) {
			statement.close();
		}
		if(getUsersStatement !=null && ! getUsersStatement.isClosed()) {
			getUsersStatement.close();
		}
		if(connection!=null && ! connection.isClosed()) {
			connection.close();
		}
	}
	
	@Override
	public void run() {
		int startId = 0;
		int endId = batchSize;
		boolean isFinished = false;
		
		while(!isFinished) {
			try {
				ResultSet result = getResultBatch(startId, endId);
				if(result.first()) {
					outputQueue.add(result);
					startId+=batchSize;
					startId+=batchSize;
				}
				else {
					isFinished = true;
				}
			} catch(Exception e) {
				// TODO logging
			}
		}
	}

}
