import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

class RelationLabel implements RelationshipType {
	private String label;
	private static HashMap<String, RelationLabel> labelTracker = new HashMap<String, RelationLabel>();

	RelationLabel(String s) {
		label = s;
	}

	@Override
	public String name() {
		return label;
	}

	public static RelationLabel getLabel(String labelName) {
		if (labelTracker.containsKey(labelName)) {
			return labelTracker.get(labelName);
		}
		RelationLabel newLabel = new RelationLabel(labelName);
		labelTracker.put(labelName, newLabel);
		return newLabel;
	}
}

/**
 * The class takes an i-graph or protein file and creates Neo-4j nodes
 * 
 * @author Lakshmi Ravi
 * 
 */
public class TransferToNeo {

	//private static String IGraph_Folder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Neo4j\\IGRAPH.graph.db\\";
	//private static String Protein_Folder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Neo4j\\DB\\backbones_1O54.grf";
	private static String IGraph_Folder;
	private static String Protein_Folder;


	private BatchInserter inserterIGraph = null;
	private BatchInserter inserterProtein = null;

	/**
	 * Writes the file data to graph database
	 * 
	 * @param proteinFile
	 *            - file to written as graph
	 * @param nodeOffset
	 *            - the node-ids are considered with the given offset
	 * @return - list of node-ids inserted from the file
	 * @throws IOException
	 */
	public List<Long> addProteinFileData(File proteinFile) throws IOException {

		List<Long> allElementNodes = new LinkedList<Long>();
		// labels = for every node, append the common label
		BufferedReader proteinFileContent = new BufferedReader(new FileReader(
				proteinFile));
		String currentLine;
		int nodeCount = Integer.parseInt(proteinFileContent.readLine());
		
		while ((currentLine = proteinFileContent.readLine()) != null
				&& nodeCount-- > 0) {
			// add nodes from the lines
			String[] lineElements = currentLine.split(" ");
			long id =  Long.parseLong(lineElements[0]);
			HashMap<String, Object> oneNodeMap = new HashMap<String, Object>();
			inserterProtein.createNode(id, oneNodeMap,  Label.label(lineElements[1]));
			allElementNodes.add(id);
		}

		// add the relations in the file
		while ((currentLine = proteinFileContent.readLine()) != null) {
			String[] lineElements = currentLine.split(" ");
			if (lineElements.length == 2) {
				long fromNode =  Long.parseLong(lineElements[0]);
				long toNode = Long.parseLong(lineElements[1]);
				// create a relation between "fromNode" -> "toNode" .
				inserterProtein.createRelationship(fromNode, toNode,
						RelationshipType.withName("ProteinBond"), null);
			}

		}
		proteinFileContent.close();
		return allElementNodes;

	}

	public List<Long> convertIgraphToGraphDb(File igraph) throws IOException, FileNotFoundException {

		List<Long> allElementNodes = new LinkedList<Long>();
		BufferedReader fileContent = new BufferedReader(new FileReader(igraph));
		String currentLine;

		while ((currentLine = fileContent.readLine()) != null) {
			String[] lineElements = currentLine.split(" ");
			if (lineElements[0].equals("e")) {
				// add a relation from the mentioned nodes
				long fromNode = Long.parseLong(lineElements[1]);
				long toNode =  Long.parseLong(lineElements[2]);
				String relationLabel = lineElements[3];
				// create a relation between "fromNode" -> "toNode" .
				inserterIGraph.createRelationship(fromNode, toNode,
						RelationLabel.getLabel(relationLabel), null);

			} else if (lineElements[0].equals("v")) {
				long id =  Long.parseLong(lineElements[1]);
				HashMap<String, Object> oneNodeMap = new HashMap<String, Object>();
				Label[] nodeLabels = new Label[lineElements.length - 2];
				for (int i = 2; i < lineElements.length; i++) {
					nodeLabels[i - 2] = Label.label(lineElements[i]);
				}
				inserterIGraph.createNode(id, oneNodeMap, nodeLabels);
				allElementNodes.add(id);

			} 

		}
		fileContent.close();
		return allElementNodes;
	}

	public void createProfileLabels(List<Long> nodes, BatchInserter inserter) {
		for (Long id : nodes) {
			List<String> currentProfile = new ArrayList<String>();
			Iterable<BatchRelationship> edges = inserter.getRelationships(id);
			Iterator<BatchRelationship> edgeIter = edges.iterator();
			//for every label in the current node
			Iterator<Label> nLabels = inserter.getNodeLabels(id).iterator();
			while(nLabels.hasNext()){
				currentProfile.add(nLabels.next().name());
			}
			
			// for every neighbor of the current node
			while (edgeIter.hasNext()) {
				BatchRelationship currEdge = edgeIter.next();
				long neighbor = currEdge.getEndNode();
				Iterable<Label> Labels = inserter.getNodeLabels(neighbor);
				Iterator<Label> labelIter = Labels.iterator();
				// append every label of the neighbor to the profile
				while (labelIter.hasNext()) {
					currentProfile.add(labelIter.next().name());
				}
			}
			currentProfile.sort(null);
			String[] profileArr = currentProfile.toArray(new String[0]);
			inserter.setNodeProperty(id, "PROFILE", profileArr);

		}
	}

	public void IterateOverFilesProteinFolder(String proteinFolder)
			throws IOException {

		File proteinFolder_directory = new File(proteinFolder);
		File[] protein_files = proteinFolder_directory.listFiles();
		for (File protein : protein_files) {
			inserterProtein = BatchInserters.inserter(new File(Protein_Folder+protein.getName()+"\\"));
			System.out.println("Inserting data from file..." + protein.getName());
			List<Long> nodes = addProteinFileData(protein);
			this.createProfileLabels(nodes, inserterProtein);
			inserterProtein.shutdown();
		}
	}

	/***
	 * Iterate over the igraph files and add them in the database
	 * 
	 * @param iGraphFolder
	 * @throws IOException
	 */
	public void IterateOverFilesIGraphFolder(String iGraphFolder)
			throws IOException {

		File igraph_directory = new File(iGraphFolder);
		File[] igraphs = igraph_directory.listFiles();
		for (File ig : igraphs) {
			inserterIGraph = BatchInserters.inserter(new File(IGraph_Folder+ig.getName()+"\\"));
			System.out.println("Inserting data from file..." + ig.getName());
			List<Long> nodes = this.convertIgraphToGraphDb(ig);
			this.createProfileLabels(nodes, inserterIGraph);
			inserterIGraph.shutdown();

		}
	}

	public static void main(String[] args) {
		TransferToNeo datatransfer = null;
		try {
			datatransfer = new TransferToNeo();
			//IGraph_Folder=args[0];
			//Protein_Folder=args[0];
			Protein_Folder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Neo4j\\DB\\";

			/*String iGraphFolder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Assignments\\A4\\iGraph\\iGraph\\";
			String iGraphQuery = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Assignments\\A4\\iGraph\\query\\";

			String proteinFolder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Assignments\\A4\\Proteins\\Proteins\\Proteins\\target\\";
			String proteinSingleFile = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Assignments\\A4\\Proteins\\Proteins\\Proteins\\sampleData\\";

			*/
			//String iGraphFolder = args[1];
			//String proteinFolder = args[2];
			String proteinFolder = "C:\\Users\\laksh\\Documents\\Graph_Databases\\Assignments\\A4\\Proteins\\Proteins\\Proteins\\sampleData\\";


			//datatransfer.IterateOverFilesIGraphFolder(iGraphFolder);
			datatransfer.IterateOverFilesProteinFolder(proteinFolder);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
