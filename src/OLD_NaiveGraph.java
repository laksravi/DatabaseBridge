import java.io.File;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.graphdb.RelationshipType;

public class OLD_NaiveGraph {
	//private static String DB_PATH = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Neo4j\\default.graph.db\\";
	private static String DB_PATH ;
	private GraphDatabaseService graphDb = new GraphDatabaseFactory()
			.newEmbeddedDatabase(new File(DB_PATH));
	private List<String> nodes = new LinkedList<String>();
	private HashMap<String, String> nodeLabelMap = new HashMap<String, String>();
	private HashMap<String, Integer> nodeBucketIdMap = new HashMap<String, Integer>();
	private ResourceIterator[] nodeBuckets = null;
	private List<Integer>[] incomingEdges = null;
	private HashMap<String, String> whereClauses = new HashMap<String, String>();
	private List<String[]> edgeLines = new LinkedList<String[]>();
	private HashSet<String> visitedEdges = new HashSet<String>();
	private HashSet<Long> visitedNodes = new HashSet<Long>();

	
	/**
	 * Pattern checking phase, given a node to start from and the bucket of neighboring nodes it could be connected to, 
	 *   1) find if the node has a relation with one of the nodes in the neighboring nodes.
	 *   2) for the node it has a relation with, find the set of edges it needs to have (List of buckets to check relation for)
	 *   3) repeat the step for the neighboring node. 
	 *   4) If the node doesn't find relation with any node in the neighbor bucket, move the current node pointer to next
	 * Checks if required edges are available from the given node U and it's corresponding neighbors
	 * @param U
	 * @param currentNodeIndex
	 * @param currEndIndex
	 * @return
	 */
	public boolean checkEdgeExists(Node U, int currentNodeIndex,int currEndIndex, ArrayList<Node> path) {
		ResourceIterator potentialV = this.nodeBuckets[currEndIndex];
		List<Node> outputPath = new LinkedList<Node>();
		outputPath.add(U);
		Iterable<Relationship> allEdges_U = U.getRelationships();
		Iterator<Relationship> iterator_edgesU = allEdges_U.iterator();
		HashSet<Long> U_Neighbors = new HashSet<Long>();
		// set the currently iterating edge as visiting edge
		String edgeName = currentNodeIndex + "_" + currEndIndex;
		visitedEdges.add(edgeName);

		while (iterator_edgesU.hasNext()) {
			Relationship rel = iterator_edgesU.next();
			U_Neighbors.add(rel.getEndNodeId());
		}

		boolean visited = false;
		while (potentialV.hasNext()) {
			Node currNeighbor = (Node) potentialV.next();
			// if the neighbors of U has the currentNode as one of it's
			// neighbors
			// and the node was not previously visited
			if (U_Neighbors.contains(currNeighbor)
					&& !(visitedNodes.contains(currNeighbor.getId()))) {
				// there is a neighbor for the node U in the EndIndex-Bucket
				// list (V)
				// existence is true , now trigger for the neighboring node
				// found --- V
				visited = true;
				// iterate through the unvisited neighbors of V
				List<Integer> neighbors_V = this.incomingEdges[currEndIndex];
				Iterator<Integer> iterNeighbors_V = neighbors_V.iterator();

				// for every neighbor required in the iterator, check if one
				// exists in the bucket
				while (iterNeighbors_V.hasNext()) 
				{
					int nextNeighbor = iterNeighbors_V.next();
					System.out.println(nextNeighbor);
					String curredgeName = currEndIndex + "_" + nextNeighbor;
					if (!visitedEdges.contains(curredgeName)) 
					{
						ArrayList<Node> neighborPath = new ArrayList<Node>();
						boolean check = checkEdgeExists(currNeighbor,
								currEndIndex, nextNeighbor, neighborPath);
						// No neighbor exists for one edge in the entire bucket
						if (check == false)
							return false;
						//if the neighbor node satisfy the conditions then add them to the current path
						
						/* if the other neighbor paths fail in the next iteration, it returns false, 
						hence the path will not be appended with parent path*/
						path.addAll(neighborPath);

					}
				}
			}
		}

		return visited;
	}

	
	/**
	 * This is the method which defines the search space of the problem
	 * getNodes method filters the nodes of our interest 
	 */
	public ResourceIterator[] getNodes() {
		ResourceIterator[] nodeBuckets = null;

		int nodeIndex = 0;
		int totalNodeBuckets = nodes.size();
		nodeBuckets = new ResourceIterator[totalNodeBuckets];

		// get all the nodes in each of the bucket specified
		for (String node : this.nodes) {
			String labelForNode = nodeLabelMap.get(node);
			String filterForNode = whereClauses.get(node);
			System.out.println("Getting nodes with label "+labelForNode+"\t"+filterForNode);

			nodeBucketIdMap.put(node, nodeIndex);

			if (!filterForNode.equals("NO_FILTER")) {
				String[] key_value = filterForNode.replaceAll("\"", "").split(
						"=");
				nodeBuckets[nodeIndex++] = graphDb.findNodes(
						Label.label(labelForNode), key_value[0], key_value[1]);
			} else {
				nodeBuckets[nodeIndex++] = graphDb.findNodes(Label
						.label(labelForNode));

			}
		}

		return nodeBuckets;

	}

	/***
	 * Utiltity : from the raw console input - create an adjaceny list to represent edge relations between the nodes.
	 */
	private void setIncomingEdges() {
		Iterator<String[]> edge = edgeLines.iterator();
		int numberNodes = nodes.size();
		incomingEdges = new LinkedList[numberNodes];
		for(int i=0;i<numberNodes;i++){
			incomingEdges[i] = new LinkedList<Integer>();
		}

		while (edge.hasNext()) {
			String[] oneEdge = edge.next();
			String start = oneEdge[0];
			String end = oneEdge[1];
			int startIndex = this.nodeBucketIdMap.get(start);
			int endIndex = this.nodeBucketIdMap.get(end);
			incomingEdges[startIndex].add(endIndex);

		}

	}

	/**
	 * gets the nodes and edge list from the raw input lines as u1 Movie NAME=x
	 * => Node-u1 label-Movie Filter name=x u1 u2 => edge u1 and u2
	 * 
	 * @param inputLines
	 */
	public void getNodeEdgeList(List<String> inputLines) {
		for (String line : inputLines) {
			String[] words = line.split(" ");
			// node is represented if there are three words
			if (words.length == 3) {
				nodes.add(words[0]);
				nodeLabelMap.put(words[0], words[1]);
				whereClauses.put(words[0], words[2]);

			} else {
				String[] edgeLine = new String[2];
				edgeLine[0] = words[0];
				edgeLine[1] = words[1];
				edgeLines.add(edgeLine);

			}
		}

	}

	/**
	 * From the input console, get the input lines
	 * 
	 * @return list of strings
	 */
	public List<String> readLines() {
		System.out
				.println("Type in the nodes with Name, Label, Filter (type NO_FILTER if there is no filter)\n Edges as Parent node and Child node\n ******\n ");
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

	public void printArrayList(ArrayList<Node> nodes){
		System.out.println("***************************************");
		for(Node n: nodes){
			System.out.println(n);
		}
		System.out.println("***************************************");
	}
	
	public void startGraphMatching(int index) {
		// start the matching beginning from the node_bucket with the given
		// nodex
		ResourceIterator startNodeBucket = this.nodeBuckets[index];
		List<Integer> incomingEdges_U = this.incomingEdges[index];
		Iterator<Integer> u_incomingEdgeIterator = incomingEdges_U.iterator();

		while (u_incomingEdgeIterator.hasNext()) {
			// for every edge u can possibly have, check all potential nodes
			int currEndIndex = u_incomingEdgeIterator.next();
			
			System.out.println("Printing Nodes that matches the problem");
			while (startNodeBucket.hasNext()) {
				// for every edge listed in that find the sequence of nodes.
				Node currentNode = (Node) startNodeBucket.next();
				ArrayList<Node> nodesofGraph = new ArrayList<Node>();
				if(this.checkEdgeExists(currentNode, index, currEndIndex, nodesofGraph))
					this.printArrayList(nodesofGraph);
			}
		}

	}

	public static void main(String[] args) {
		if(args.length != 1){
			System.out.println("Enter the path of database directory as an argument");
			return;
		}
		System.out.println("Extracting db from the path..."+args[0]);
		DB_PATH= args[0];
		OLD_NaiveGraph naiveMatcher = new OLD_NaiveGraph();
		List<String> IoLines = naiveMatcher.readLines();

		try (Transaction tx = naiveMatcher.graphDb.beginTx()) {
			naiveMatcher.getNodeEdgeList(IoLines);

			// set the potential-nodes in the corresponding bucket of nodes
			naiveMatcher.nodeBuckets = naiveMatcher.getNodes();
			naiveMatcher.setIncomingEdges();

			// pick one edge randomly and choose a node and it's buckets
			naiveMatcher.startGraphMatching(0);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
