package uk.ac.hw.DataMigrator.dto;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class MovieJsonDto  {
	private String movieId;
	private String movieTitle;
	
	private List<String> genres;
	private List<Review> reviews;
	
	public MovieJsonDto(String movieId, String title, List<String> genres, List<String> reviews) {
		this.movieId = movieId;
		this.movieTitle = movieId;
		this.genres = genres;
		this.reviews = this.reviews;
	}
	private class Review {
		private int id;
		private int rating;
		
		public Review(int id, int rating) {
			this.setId(id);
			this.setRating(rating);
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public int getRating() {
			return rating;
		}

		public void setRating(int rating) {
			this.rating = rating;
		}
	}
}
