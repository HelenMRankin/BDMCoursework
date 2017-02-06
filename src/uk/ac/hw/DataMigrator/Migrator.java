package uk.ac.hw.DataMigrator;

import java.sql.ResultSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import uk.ac.hw.DataMigrator.database.MongoDbConnector;
import uk.ac.hw.DataMigrator.database.SqlConnector;
import uk.ac.hw.DataMigrator.parse.DataParser;
import uk.ac.hw.DataMigrator.parse.MoviesParser;

public class Migrator {

	public static void main(String[] args) throws Exception {
		ConcurrentLinkedQueue<ResultSet> moviesResultQueue = new ConcurrentLinkedQueue<ResultSet>();
		ConcurrentLinkedQueue<String> moviesJsonQueue = new ConcurrentLinkedQueue<String>();
		SqlConnector connector = new SqlConnector();
		Runnable sqlMoviesRunnable = connector.getMoviesRunnable(moviesResultQueue, 500);
		
		MoviesParser parser = new MoviesParser(moviesResultQueue, moviesJsonQueue);
		new Thread(sqlMoviesRunnable).start();
		new Thread(parser).start();
		
		

	}

}
