import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

/**
 * A class created to test the transactions ACID property of database
 * @author Lakshmi Ravi
 *
 */
public class TransactionCheck {
	private static final String JDBC_DRIVER_NAME="com.mysql.jdbc.Driver";
	private static final String CONNECTION_STRING="jdbc:mysql://localhost:3306/";
	private Connection connectionRequired;
	private int totalRoles;
	
	
	
	
	/**
	 * A test method for role swap
	 * Actor -1 doing a role in movie X wants to swap his role with Actor-2 doing another role in a movie Y
	 * This is a transaction and is tested for consistency.
	 * @param databaseName : where the tables are located
	 * @param userName : access user name
	 * @param password : access password
	 * @throws SQLException 
	 */
	public void test_transaction(String databaseName, String userName, String password)  {
		
		try {
			Class.forName(JDBC_DRIVER_NAME);
			
			//create connection with database.
			connectionRequired = DriverManager.getConnection(CONNECTION_STRING+databaseName, userName, password);
			//statement object to deal with the connection to database. 
			Statement statement = connectionRequired.createStatement();
			System.out.println("Established connection with the database");

			//turn off auto commit and create a save point.
			connectionRequired.setAutoCommit(false);
			Savepoint savepoint = connectionRequired.setSavepoint("Role-Exchange-Begin");
			System.out.println("Attempting to exchange the role of actor id-100 for movie 248215 with actor-id 101 for movie-id 1");
			
			
			//To check for consistency of database state
			//A+B in the beginning is same in the end
			//In a role swap, initial count of roles held is same in the end.
			String consistencyChecker=("select count(*) from roles where actor_id = 100 or actor_id = 101");
			ResultSet results = statement.executeQuery(consistencyChecker);
			int totalRolesForBoth = 0;
			while(results.next()){
				totalRolesForBoth = Integer.parseInt(results.getString(1));
			}
			System.out.println("Initally the total roles by X and Y are"+totalRolesForBoth);
			
			/*
			 Exchange roles from actors with actor-id 100 for movie-id 248215
			with actor-id 101 for movie-id 1 
			*/
			int updatesFrom=statement.executeUpdate("update roles set actor_id = 100 where movie_id=248215 and actor_id=101");
			
			//This is the introduced error : there is no role for actor-100 in movie-1, hence no rows are updated
			int updatesTo=statement.executeUpdate("update roles set actor_id = 100 where movie_id=1 and actor_id=101");

			//update the status of role-exchange.
			if(updatesFrom != updatesTo){
				System.out.println("Issues on transaction, aborting the roll-change process");
				connectionRequired.rollback(savepoint);
				connectionRequired.commit();
			}
			else if(updatesFrom == 0 && updatesTo == 0){
				System.out.println("No role exists to exchange");
			}
			else{
			System.out.println("Sucessfully exchanged"+ updatesFrom+"  role between them");
			connectionRequired.commit();
			}
			
			
			//Check for consistency
			results = statement.executeQuery(consistencyChecker);
			int newTotalRolesForBoth = 0;
			while(results.next()){
				newTotalRolesForBoth = Integer.parseInt(results.getString(1));
			}
			System.out.println("Final the total roles by X and Y are"+totalRolesForBoth);
			System.out.println("Consistency Maintained..."+ (totalRolesForBoth == newTotalRolesForBoth));
			
			//terminate the connection
			connectionRequired.close();
		} 
		
		catch (Exception e) {
			e.printStackTrace();
			try {
				connectionRequired.abort(null);
			} catch (SQLException e1) {
				System.out.println("Error when aborting a transaction"+e);
			}
		}
		
	}
	
	
	

	public static void main(String args[]) {
		TransactionCheck transationChecker = new TransactionCheck();
		//if the necessary arguments are not provided, exit
		if(args.length < 3){
			System.out.println("Usage java Transation_Check <Database Name> <User name> <Password>");
			return;
		}
		//test a transaction
		
		transationChecker.test_transaction(args[0], args[1], args[2]);
	}


	
}