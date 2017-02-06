package uk.ac.hw.DataMigrator.parse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractParser implements Runnable {
	private ConcurrentLinkedQueue<ResultSet> inputQueue;
	private ConcurrentLinkedQueue<String> outputQueue;
	private boolean isRunning = true;
	
	public AbstractParser (ConcurrentLinkedQueue<ResultSet> inputQueue, ConcurrentLinkedQueue<String> outputQueue) {
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
	}
	
	public void setRunning(boolean isRunning) {
		this.isRunning = isRunning;
	}
	
	@Override
	public void run() {
		while (isRunning) {
			while (!inputQueue.isEmpty()) {
				inputQueue.stream().forEach( result -> { 
					try {
						result.beforeFirst();
						parseResultSet(result);
					} catch (Exception e) {
						// TODO logging
					}
				});
			}
			try {
				inputQueue.wait(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				break;
			}
		}
	}

	
	protected void parseResultSet(ResultSet result) throws SQLException {
		if(result.next()) {
			outputQueue.add(parseToJson(result));
			parseResultSet(result);
		}
		else {
			result.close();
		}
	}
	
	protected abstract String parseToJson(ResultSet result);
}
