package uk.ac.hw.DataMigrator.parse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataParser {
	private ConcurrentLinkedQueue<ResultSet> inputQueue;
	private ConcurrentLinkedQueue<String> outputQueue;

	public DataParser(ConcurrentLinkedQueue<ResultSet> inputQueue, ConcurrentLinkedQueue<String> outputQueue) {
		this.inputQueue = inputQueue;
		this.outputQueue = outputQueue;
	}
	
	public Runnable parseMovies(ConcurrentLinkedQueue<ResultSet> inputQueue, ConcurrentLinkedQueue<String> outputQueue) {
		return new Runnable() {

			@Override
			public void run() {
				while (true) {
					while (!inputQueue.isEmpty()) {
						ResultSet result = inputQueue.poll();
						if (result != null) {
							try {
								parseResultSet(result, outputQueue);
							} catch (Exception e) {
								// TODO logging
							}
						}
					}
				}
			}
		};
	}
	
	public void parseResultSet(ResultSet result, ConcurrentLinkedQueue<String> output) throws SQLException {
		if(result.next()) {
			parseMoviesToJson(result);
			output.add(parseMoviesToJson(result));
			parseResultSet(result, output);
		}
	}

}
