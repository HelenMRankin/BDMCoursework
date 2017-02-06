package uk.ac.hw.DataMigrator.parse;

import java.sql.ResultSet;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MoviesParser extends AbstractParser {

	public MoviesParser(ConcurrentLinkedQueue<ResultSet> inputQueue,
			ConcurrentLinkedQueue<String> outputQueue) {
		super(inputQueue, outputQueue);
	}

	@Override
	public String parseToJson(ResultSet result) {
		// TODO Auto-generated method stub
		return null;
	}

}
