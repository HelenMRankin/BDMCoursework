package uk.ac.hw.DataMigrator.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

import uk.ac.hw.DataMigrator.parse.DataParser;

import com.mysql.jdbc.StringUtils;

/**
 * Connector to SQL database
 */
public class SqlConnector implements AutoCloseable {
	private Connection connection;
	private PreparedStatement getMoviesStatement;
	private PreparedStatement getUsersStatement;
	
	private static final String DB_URL = "";
	private static final String USER = "";
	private static final String GET_MOVIES_STRING = "";
	private static final String GET_USERS_STRING = "";
	
	public SqlConnector() throws Exception{
		this.connection = DriverManager.getConnection(DB_URL, USER, readPasswordFromArgs());
		this.getMoviesStatement = connection.prepareStatement(GET_MOVIES_STRING);
		this.getUsersStatement = connection.prepareStatement(GET_USERS_STRING);
	}
	
	public ResultSet getMovies(int startId, int endId) throws SQLException {
		getMoviesStatement.clearParameters();
		getMoviesStatement.setInt(1, startId);
		getMoviesStatement.setInt(2, endId);
		return getMoviesStatement.executeQuery();
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
		if(getMoviesStatement != null && !getMoviesStatement.isClosed()) {
			getMoviesStatement.close();
		}
		if(getUsersStatement !=null && ! getUsersStatement.isClosed()) {
			getUsersStatement.close();
		}
		if(connection!=null && ! connection.isClosed()) {
			connection.close();
		}
	}
	
	public void getResults(PreparedStatement statement, ConcurrentLinkedQueue<ResultSet> outputQueue, int batchSize) {
		int startId = 0;
		int endId = batchSize;
		boolean isFinished = false;
		
		while(!isFinished) {
			try {
				
				statement.clearParameters();
				statement.setInt(1, startId);
				statement.setInt(2, endId);
				
				ResultSet result = statement.executeQuery();
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
	
	public Runnable getMoviesRunnable(ConcurrentLinkedQueue<ResultSet> outputQueue, int batchSize) {
		return new Runnable() {
			@Override
			public void run() {
				getResults(getMoviesStatement, outputQueue, batchSize);
			}
		};
	}
	
	public Runnable getUsersRunnable(ConcurrentLinkedQueue<ResultSet> outputQueue, int batchSize) {
		return new Runnable() {
			@Override
			public void run() {
				getResults(getUsersStatement, outputQueue, batchSize);
			}
		};
	}

}
