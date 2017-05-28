import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;


import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.graphdb.RelationshipType;

public class OLD_TRANSFER {

	private static enum RelTypes implements RelationshipType {
		
		ACTED_IN, MOVIE_GENRE, DIRECTED_BY, DIRECTOR_GENRE
	}
	
	
	
	//private static String DB_PATH = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Neo4j\\default.graph.db\\";
	private static String DB_PATH = "";
	private static final String JDBC_DRIVER_NAME = "com.mysql.jdbc.Driver";
	private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
	private static Connection connectionRequired;
	private static Statement statement;

	// the values to be added for every unique id in the table, so that the
	// value is globally unique
	private static final long ACTOR_APPEND = 10000000;
	private static final long DIRECTOR_APPEND = 20000000;
	private static final long MOVIE_APPEND = 30000000;
	private static final long GENRE_APPEND = 40000000;

	/**
	 * A method to connect with MY-SQL to load the data
	 * 
	 * @param databaseName
	 *            , name of MY-SQL database
	 * @param userName : user-name
	 * @param password
	 * @throws Exception
	 *             , when the connection is not established
	 */
	public static void connectWithMySql(String databaseName, String userName,
			String password) throws Exception {

		Class.forName(JDBC_DRIVER_NAME);

		// create connection with database.
		connectionRequired = DriverManager.getConnection(CONNECTION_STRING
				+ databaseName, userName, password);
		// statement object to deal with the connection to database.
		statement = connectionRequired.createStatement();
	}

	public static ResultSet getRequiredData(String tableName, String id)
			throws Exception {
		// get the entire data set from the table
		String currentTableData = "Select * from   " + tableName
				+ " where id =" + id;
		ResultSet actordata = statement.executeQuery(currentTableData);
		return actordata;
	}

	public static ResultSet getRequiredData(String tableName) throws Exception {
		// get the particular data set from the table
		String currentTableData = "Select * from   " + tableName;
		ResultSet actordata = statement.executeQuery(currentTableData);
		return actordata;
	}

	
	private static ResultSet getUniqueData(String column, String tableName) throws SQLException {
		// get the particular data set from the table
				String currentTableData = "Select distinct("+ column+") from   " + tableName;
				ResultSet actordata = statement.executeQuery(currentTableData);
				return actordata;
	}
	
	
	/**
	 * 
	 * @throws Exception
	 */
	public static void transferEntities() throws Exception {
		Label actorLabel = Label.label("Actor");
		Label movieLabel = Label.label("Movie");
		Label directorLabel = Label.label("Director");
		Label genreLabel = Label.label("Genre");
		
		File dbpath = new File(DB_PATH);
		dbpath.exists();
		BatchInserter inserter = BatchInserters.inserter(dbpath);
		
		
		//  ACTOR DATA INSERTION
		ResultSet actors = getRequiredData("ACTORS");
		
		while (actors.next()) {
			int actorId = actors.getInt("ID");
			long actorglobalId = ACTOR_APPEND + actorId;
			String firstName = actors.getString("FIRST_NAME");
			String lastName = actors.getString("LAST_NAME");
			HashMap<String, Object> oneActor = new HashMap<String, Object>();
			oneActor.put("FIRST_NAME", firstName);
			oneActor.put("LAST_NAME", lastName);
			inserter.createNode(actorglobalId, oneActor, actorLabel);
		}
		System.out.println("Actors Data Transferred");

		// MOVIE DATA INSERTION 
		ResultSet movies = getRequiredData("MOVIES");
		while (movies.next()) {
			int movieId = movies.getInt("ID");
			long globalMovieId = MOVIE_APPEND + movieId;
			String mName = movies.getString("NAME");
			int year = movies.getInt("YEAR");

			HashMap<String, Object> oneMovieMap = new HashMap<String, Object>();
			oneMovieMap.put("TITLE", mName);
			oneMovieMap.put("YEAR", year);
			inserter.createNode(globalMovieId, oneMovieMap, movieLabel);
		}
		System.out.println("Movies Data Transferred");
		
		
		
		// MOVIE DATA INSERTION 
		ResultSet directors = getRequiredData("DIRECTORS");
		while (directors.next()) {
			int directorId = directors.getInt("ID");
			long globaldirectorId = DIRECTOR_APPEND + directorId;
			String firstName = directors.getString("FIRST_NAME");
			String lastName = directors.getString("LAST_NAME");

			HashMap<String, Object> oneDrMap = new HashMap<String, Object>();
			oneDrMap.put("FIRST_NAME", firstName);
			oneDrMap.put("LAST_NAME", lastName);
			inserter.createNode(globaldirectorId, oneDrMap, directorLabel);
		}
		System.out.println("Directors Data Transferred");
		
		
		// GENRE DATA INSERTION
		HashMap<String, Long> genreNameIdMapper = new HashMap<String, Long>();
		ResultSet genres = getUniqueData("GENRE","MOVIES_GENRES");
		int genreCounter =1;
		while (genres.next()) {
			String genre = genres.getString("GENRE");
			long globalGenreId = GENRE_APPEND + genreCounter;
			HashMap<String, Object> Onegenre = new HashMap<String, Object>();
			Onegenre.put("GENRES", genre);
			inserter.createNode(globalGenreId, Onegenre, genreLabel);
			genreNameIdMapper.put(genre, globalGenreId);
			genreCounter++;
		}
		System.out.println("Genres Data Transferred");

		//RELATE ROLES
		ResultSet roles = getRequiredData("ROLES");
		// insert the roles one by one
		while (roles.next()) {
			int movie_id = roles.getInt("MOVIE_ID");
			int actor_id = roles.getInt("ACTOR_ID");
			String role = roles.getString("ROLE");
			// get the unique ID
			long movieNode = MOVIE_APPEND + movie_id;
			long actorNode = ACTOR_APPEND + actor_id;

			HashMap<String, Object> roles_attributes = new HashMap<String, Object>();
			roles_attributes.put("ROLE", role);
			inserter.createRelationship(actorNode, movieNode,
					RelTypes.ACTED_IN, roles_attributes);
		}
		System.out.println("Roles Data Transferred");

		int counter=0;
		// ================ RELATE MOVIES_DIRECTORS
				ResultSet movie_directors = getRequiredData("MOVIES_DIRECTORS");
				// insert the movie directors one by one
				while (movie_directors.next()) 
				{
					int movie_id = movie_directors.getInt("MOVIE_ID");
					int director_id = movie_directors.getInt("DIRECTOR_ID");
					// get the globally unique ID
					long movieNode = MOVIE_APPEND + movie_id;
					long directorNode = DIRECTOR_APPEND + director_id;
					if(inserter.nodeExists(directorNode) && inserter.nodeExists(movieNode))
						inserter.createRelationship(movieNode, directorNode,RelTypes.DIRECTED_BY, null);
					else
						counter++;
				}
				System.out.println("Movie-Directors Data Transferred"+counter);

				// ************* RELATE MOVIES GENRES
				ResultSet movie_genres = getRequiredData("MOVIES_GENRES");
				// insert the movie directors one by one
				while (movie_genres.next()) 
				{
					int movie_id = movie_genres.getInt("MOVIE_ID");
					String genre = movie_genres.getString("GENRE");
					// get the globally unique ID
					long movieNode = MOVIE_APPEND + movie_id;
					if(genreNameIdMapper.containsKey(genre))
					{
					long genreNode = genreNameIdMapper.get(genre);
					//System.out.println(inserter.nodeExists(genreNode) +"\t"+ inserter.nodeExists(movieNode));
					if(inserter.nodeExists(genreNode) && inserter.nodeExists(movieNode)){
						//System.out.println("SOMETHING");
						inserter.createRelationship(movieNode, genreNode,RelTypes.MOVIE_GENRE, null);
					}
					else{
						counter++;
						}
					}
					else
							counter++;
				}
				System.out.println("Movie Genres Transferred"+counter);

				// ************* RELATE DIRECTORS WITH GENRES
				ResultSet director_genres = getRequiredData("DIRECTORS_GENRES");
				// insert the movie directors one by one
				while (director_genres.next()) 
				{
					int director_id = director_genres.getInt("DIRECTOR_ID");
					String genre = director_genres.getString("GENRE");
					// get the globally unique ID
					long directorNode = DIRECTOR_APPEND + director_id;
					if (genreNameIdMapper.containsKey(genre))
					{
					long genreNode = genreNameIdMapper.get(genre);
					//System.out.println(inserter.nodeExists(directorNode) +"\t"+ inserter.nodeExists(genreNode));
					if(inserter.nodeExists(directorNode) && inserter.nodeExists(genreNode))
					{
								inserter.createRelationship(directorNode, genreNode,RelTypes.DIRECTOR_GENRE, null);
					}
					else
						counter++;
					}
					else{
						counter++;
					}
				}
				
				System.out.println("Director-Genres Data Transferred"+counter);
				System.out.println(counter+"   FAILED*****************");
				inserter.shutdown();
				System.out.println("Transfer complete");
		
	}

	

	public static void main(String[] args) {
		try {
			if(args.length < 4){
				System.out.println("Enter the following for migration \n SQL-db name \t MySql user-name \t MySql password \t Neo4jDBPath");
				return;
			}
				System.out.println("Migrating db.."+args[0]+"\t user-name.."+args[1]+"\t password.."+args[2]+"\n Neo4j path.."+args[3]);
			connectWithMySql(args[0], args[1], args[2]);
			DB_PATH=args[3];
			transferEntities();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
