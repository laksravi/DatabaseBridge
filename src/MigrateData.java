import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;

public class MigrateData {

	private static final String JDBC_DRIVER_NAME = "com.mysql.jdbc.Driver";
	private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
	private Connection connectionRequired;
	private String getTableData = "Select * from   ";
	private Statement statement;
	private MongoDatabase mongoDatabase;

	public void connectWithMySql(String databaseName, String userName,
			String password) throws Exception {

		Class.forName(JDBC_DRIVER_NAME);

		// create connection with database.
		connectionRequired = DriverManager.getConnection(CONNECTION_STRING
				+ databaseName, userName, password);
		// statement object to deal with the connection to database.
		statement = connectionRequired.createStatement();
		MongoClient mongoClient = new MongoClient();
		mongoDatabase = mongoClient.getDatabase("Lakshmi_Hw_2");
		System.out.println("Established connection with the database");
	}

	public ResultSet getRequiredData(String tableName) throws Exception {
		String currentTableData = getTableData + tableName ;
		System.out.println(currentTableData);
		ResultSet actordata = statement.executeQuery(currentTableData);
		return actordata;
	}

	public void tranferDataNormalized() throws Exception {

		// get contents from Actors table and populate the actors database in
		// Mongo-db
		MongoCollection<Document> MongoactorTable = mongoDatabase
				.getCollection("Actors");
		// get data from table-actor
		ResultSet actorData = getRequiredData("Actors");
		while (actorData.next()) {

			String name = actorData.getString("first_name")
					+ actorData.getString("last_name");
			String gender = actorData.getString("gender");
			int id = actorData.getInt("id");
			Document actorDocument = new Document();
			actorDocument.append("Name", name);
			actorDocument.append("id", id);
			actorDocument.append("gender", gender);
			MongoactorTable.insertOne(actorDocument);
		}

		// get contents from Movies table and populate the Movies database in
		// Mongo-db
		MongoCollection<Document> MongoMoviesTable = mongoDatabase
				.getCollection("Movies");
		ResultSet moviesData = getRequiredData("Movies");
		while (moviesData.next()) {

			String name = moviesData.getString("name");
			int id = moviesData.getInt("id");
			int year = moviesData.getInt("year");
			int rank = moviesData.getInt("rank");
			Document movieDocument = new Document();
			movieDocument.append("id", id);
			movieDocument.append("name", name);
			movieDocument.append("year", year);
			movieDocument.append("rank", rank);
			MongoMoviesTable.insertOne(movieDocument);

		}

		// get contents from directors table and insert them into mongo-db
		// table.
		MongoCollection<Document> MongoDirectorsTable = mongoDatabase
				.getCollection("Directors");
		ResultSet directorsData = getRequiredData("Directors");
		while (directorsData.next()) {

			String first_name = directorsData.getString("first_name");
			String last_name= directorsData.getString("last_name");
			int id = directorsData.getInt("id");
			Document directorDocument = new Document();
			directorDocument.append("id", id);
			directorDocument.append("first_name", first_name);
			directorDocument.append("last_name", last_name);
			MongoDirectorsTable.insertOne(directorDocument);

		}

		// update roles in the Mongo-db table
		MongoCollection<Document> MongoRolesTable = mongoDatabase
				.getCollection("Roles");
		ResultSet rolesData = getRequiredData("roles");
		while (rolesData.next()) {
			int actor_id = rolesData.getInt("actor_id");
			int movie_id = rolesData.getInt("movie_id");
			String role = rolesData.getString("role");

			Document actor_document = MongoactorTable.find(
					new Document("id", actor_id)).first();
			if(actor_document == null)
				continue;
			Object actor_object = actor_document.get((Object) "_id");

			Document movie_document = MongoMoviesTable.find(
					new Document("id", movie_id)).first();
			if(movie_document == null)
				continue;
			Object movie_object = movie_document.get((Object) "_id");
			Document role_doc = new Document();
			role_doc.append("role", role);
			role_doc.append("movie_id", movie_object);
			role_doc.append("actor_id", actor_object);

			MongoRolesTable.insertOne(role_doc);
		}

		// update Movie-genres in the Mongo-db table
		MongoCollection<Document> MongoMovieGenresTable = mongoDatabase
				.getCollection("Movie_Genres");
		ResultSet movieGenres = getRequiredData("movies_genres");

		while (movieGenres.next()) {
			int movie_id = movieGenres.getInt("movie_id");
			String genre = movieGenres.getString("genre");

			Document movie_document = MongoMoviesTable.find(
					new Document("id", movie_id)).first();
			
			if(movie_document == null)
				continue;
			
			Object movie_object = movie_document.get((Object) "_id");
			Document movie_genre_doc = new Document();
			movie_genre_doc.append("genre", genre);
			movie_genre_doc.append("movie_id", movie_object);

			MongoMovieGenresTable.insertOne(movie_genre_doc);
		}

		// update Director -genres in the Mongo-db table
		MongoCollection<Document> MongoDirectorGenresTable = mongoDatabase
				.getCollection("Director_Genres");
		ResultSet directorGenres = getRequiredData("directors_genres ");

		while (directorGenres.next()) {
			int director_id = directorGenres.getInt("director_id");
			String genre = directorGenres.getString("genre");

			Document director_document = MongoDirectorsTable.find(
					new Document("id", director_id)).first();
			if(director_document == null)
				continue;
			Object director_object = director_document.get((Object) "_id");
			Document director_genre_doc = new Document();
			director_genre_doc.append("genre", genre);
			director_genre_doc.append("director_id", director_object);

			MongoDirectorGenresTable.insertOne(director_genre_doc);
		}
		
		// update Director -Movies in the Mongo-db table
				MongoCollection<Document> MongoMovieDirectorTable = mongoDatabase
						.getCollection("Movie_Directors");
				ResultSet movieDirectors = getRequiredData("Movies_Directors ");

				while (movieDirectors.next()) {
					int director_id = movieDirectors.getInt("director_id");
					int movie_id = movieDirectors.getInt("movie_id");
					System.out.println(""+director_id+" "+movie_id);
					Document director_document = MongoDirectorsTable.find(
							new Document("id", director_id)).first();
					Document movie_document = MongoMoviesTable.find(
							new Document("id", movie_id)).first();
					
					if(director_document == null || movie_document == null)
						continue;
					Object director_object = director_document.get((Object) "_id");
					Object movie_object = movie_document.get((Object) "_id");
					Document director_movie_doc = new Document();
					director_movie_doc.append("movie_id", movie_object);
					director_movie_doc.append("director_id", director_object);

					MongoMovieDirectorTable.insertOne(director_movie_doc);
				}
		

	}

	public void tranferDataUnNormalized() throws Exception {

		String getAllData = "roles join actors on roles.actor_id = actors.id"
				+ " join movies on roles.movie_id = movies.id"
				+ " join movies_directors on movies_directors.movie_id = movies.id"
				+ " join directors on directors.id = movies_directors.director_id"
				+ " join movies_genres on movies.id = movies_genres.movie_id"
				+ " join directors_genres on movies_genres.genre = directors_genres.genre and"
				+ " directors_genres.director_id = directors.id and"
				+ " directors_genres.director_id = movies_directors.director_id";

		ResultSet entiredbData = getRequiredData(getAllData);
		MongoCollection<Document> MongoEntireDb = mongoDatabase
				.getCollection("EntireIMDB");
		while (entiredbData.next()) {
			// get actor details
			int actor_id = entiredbData.getInt("actors.id");
			String actor_first_name = entiredbData
					.getString("actors.first_name");
			String actor_last_name = entiredbData.getString("actors.last_name");
			String actor_gender = entiredbData.getString("actors.gender");
			// embed in a document
			Document actor = new Document();
			actor.append("actor_id", actor_id);
			actor.append("actor_first_name", actor_first_name);
			actor.append("actor_last_name", actor_last_name);

			// get movie details
			int movie_id = entiredbData.getInt("Movies.id");
			String movie_name = entiredbData.getString("movies.name");
			int movie_year = entiredbData.getInt("Movies.year");
			int movie_rank = entiredbData.getInt("Movies.rank");
			Document movie = new Document();
			movie.append("movie_id", movie_id);
			movie.append("movie_name", movie_name);
			movie.append("movie_year", movie_year);
			movie.append("movie_rank", movie_rank);

			// get directors details
			int director_id = entiredbData.getInt("directors.id");
			String director_first_name = entiredbData
					.getString("directors.first_name");
			String director_last_name = entiredbData
					.getString("directors.last_name");

			// embed in a director document
			Document director = new Document();
			director.append("director_id", director_id);
			director.append("director_first_name", director_first_name);
			director.append("director_last_name", director_last_name);

			// get_movie_genre_Details
			String movie_genre = entiredbData.getString("movies_genres.genre");

			// get the director's connected genre
			float directors_probability_of_genre = entiredbData
					.getFloat("directors_genres.prob");

			// find the role done by that actor in a movie.
			String role = entiredbData.getString("roles.role");

			Document cumulative = new Document();
			cumulative.append("actor", actor);
			cumulative.append("role_done", role);
			cumulative.append("movie", movie);
			cumulative.append("director", director);
			cumulative.append("genre", movie_genre);
			cumulative.append("chances_this_genre",
					directors_probability_of_genre);
			MongoEntireDb.insertOne(cumulative);

		}

	}

	public static void main(String[] args) {
		MigrateData migration = new MigrateData();
		try {
			if(args.length < 3){
				System.out.println("Usage java MigrateData <MySQL Database Name> <User name> <Password>");
				return;
			}
			//test a transaction
						
			migration.connectWithMySql(args[0], args[1], args[2]);
			migration.tranferDataNormalized();
			migration.tranferDataUnNormalized();
			migration.connectionRequired.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
