import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Parser {
	private HashMap<String, String> NodeLabelInfo = new HashMap<String, String>();
	private List<String> nodes = new LinkedList<String>();
	private HashMap<String, String> nodeLabelMap = new HashMap<String, String>();
	private List<String> whereClauses = new LinkedList<String>();
	private List<String[]> edgeLines = new LinkedList<String[]>();


	public String findCypherQueries(List<String> inputLines) {
		String query = "MATCH ";
		
		for (String line : inputLines) {
			String[] words = line.split(" ");
			// node is represented if there are three words
			if (words.length == 3) {
				nodes.add(words[0]);
				nodeLabelMap.put(words[0], words[1]);
				whereClauses.add(words[2]);

			} else {
				String[] edgeLine = new String[2];
				edgeLine[0] = words[0];
				edgeLine[1] = words[1];
				edgeLines.add(edgeLine);
				//edges have filter count 
				/*if (words.length ==5){
					String newNodeObject = newObjectPrefix+newObjectCounter++;
					groupby += words[2]+",";
					groupby += "   count("+words[3]+") AS "+ newNodeObject;
					String extendedWhere = newNodeObject+words[4];
					extendedWhereClauses.add(extendedWhere);
					
				}*/
			}
		}

		Iterator<String> nodePtr = nodes.iterator();
		Iterator<String[]> edgePtr = edgeLines.iterator();
		Iterator<String> whereClausePtr = whereClauses.iterator();

		while (edgePtr.hasNext()) {
			String[] edgeNames = edgePtr.next();
			String Label_0 = nodeLabelMap.get(edgeNames[0]);
			String Label_1 = nodeLabelMap.get(edgeNames[1]);
			// add the edges to the query
			query += "   " + "(" + edgeNames[0] + ":" + Label_0 + ")--("
					+ edgeNames[1] + ":" + Label_1 + ")";

			// append a comma
			if (edgePtr.hasNext())
				query += ",";

		}

		query += "   WHERE";
		String returnNodes = "";
		while (nodePtr.hasNext() && whereClausePtr.hasNext()) {
			String nodeName = nodePtr.next();
			String nodes_whereClause = whereClausePtr.next();

			//if there is no-filter don't append the where clause
			if (!nodes_whereClause.equals("NO_FILTER")) {
				String nodes_whereClauseUpdated = nodes_whereClause.replace("_AND_", " AND ");
				//check if there is a node next to append "AND" or not
				if (whereClausePtr.hasNext()) {
					query += " " + nodeName + "." + nodes_whereClause + " AND ";
					returnNodes += nodeName + ",";

				} else {
					query += "  " + nodeName + "." + nodes_whereClause;
					returnNodes += nodeName;
				}

			}

		}

		query += "  RETURN  ";
		query += returnNodes;
		return query;
	}

	public List<String> readLines() {
		System.out
				.println("Type in the nodes with Name, Label, Filter \n\t(type NO_FILTER if there is no filter connect using _AND_ for multiple filters)\n Edges as Parent node and Child node ");
		List<String> Iolines = new LinkedList<String>();
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (line.equals("END"))
				break;
			Iolines.add(line);
		}

		return Iolines;
	}

	public static void main(String[] args) {
		Parser cypherConverter = new Parser();
		List<String> ioLines = cypherConverter.readLines();
		String query = cypherConverter.findCypherQueries(ioLines);
		System.out.println(query);
	}

}
