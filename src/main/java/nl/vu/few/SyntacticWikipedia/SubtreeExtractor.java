package nl.vu.few.SyntacticWikipedia;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.getopt.util.hash.FNV164;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

/*
 * This class takes Wikipedia plaintext as input (one per line, with wikipedia document ID as key)
 * and creates creates all of the quadarcs for each sentence.
 * WARNING: output can be quite large, filtering out the bad arcs is very important.
 */

public class SubtreeExtractor {
	
	static final Logger log = LoggerFactory.getLogger(SubtreeExtractor.class);

	public static class Mapper extends Action {
		private FNV164 hasher; // very fast collision-rare 64bit hash (FNV1 implementation)
		
		Properties props;
		long sumPreproctime;
		long ndocs;
		long nsentences;
		
		private int treeSize = 5;
		private String treeName = "quadarcs";
		
		@Override
		public void startProcess(ActionContext context) throws Exception {
			super.startProcess(context);
			// configure CoreNLP pipeline
			props = new Properties();
			props.put("annotators", "tokenize,ssplit,pos,depparse");
			// auxiliary variables
			sumPreproctime = 0;
			nsentences = ndocs = 0;
			switch (treeSize) {
				case 5:
					treeName = "quadarcs";
					break;
				case 4:
					treeName = "triarcs";
					break;
				case 3:
					treeName = "biarcs";
					break;
				default:
					treeName = "arcs";
			}
		}

		@Override
		public void process(Tuple tuple, ActionContext context,	ActionOutput actionOutput) throws Exception {
			hasher = new FNV164();
			
			long time = System.currentTimeMillis();
			// expected input is: document_ID<tab>document_text
			String documentText = ((TString) tuple.get(0)).getValue();
			
			// get the wikipedia DocumentID as being the first integer on the line
			Scanner scan = new Scanner(documentText);
			int wikiDocId = scan.nextInt();
			scan.close();
			// remove the DocumentID from content of article
			int skipIndex = (int) Math.log10(wikiDocId) + 1;
			documentText = documentText.substring(skipIndex);
				
			// init CoreNLP pipeline
	        Annotation annotation = new Annotation(documentText);
	        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
	        // start CoreNLP pipeline
	        pipeline.annotate(annotation);
	        // get sentences from CoreNLP pipeline (created by ssplit annotator)
	        List<CoreMap> sentences =  annotation.get(CoreAnnotations.SentencesAnnotation.class);
	        
	        if (sentences != null) {
	        	for (CoreMap sentence : sentences) {	        		
	                // compute sentence hash
	                String originalSentence = sentence.get(CoreAnnotations.TextAnnotation.class);
	                hasher.update(originalSentence);
	                long sentenceHash = hasher.getHash();
	                // skip very long sentences
	                if (sentence.size() > 40) {
	        			//actionOutput.output(new TString("excluded"), new TString(sentenceHash+"\t"+wikiDocId+"\t"+originalSentence));
	                	continue;
	        		}
	                // output annotated sentence
	                writeSentence(actionOutput, wikiDocId, sentenceHash, sentence);
	                // get syntactic parse tree from CoreNLP pipeline (created by depparse annotator)
	                SemanticGraph sg = sentence.get(SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);
	                // skip very long sentences
	                if (sg.vertexSet().size() > 40) {
	        			//actionOutput.output(new TString("excluded"), new TString(sentenceHash+"\t"+wikiDocId+"\t"+originalSentence));
	                	continue;
	        		}
	                // remove punctuation, gibberish (i.e. malformed HTML tags) and stopwords
	                removeUndesired(sg, true);
	                // extract all syntactic tree fragments of given size (in our case, quadarcs)
	                HashSet<ArrayList<IndexedWord>> arcs = allSubtrees(sg, treeSize);
	                // write output
	                writeSubtrees(actionOutput, treeName, wikiDocId, sentenceHash, arcs);
	        	}            
	        }
		}
		
		public void removeUndesired(SemanticGraph sg, boolean removeStopwords) {
			List<IndexedWord> sortedNodes = sg.vertexListSorted();
	    	for (IndexedWord node : sortedNodes) {
	    		String pos = node.get(CoreAnnotations.PartOfSpeechAnnotation.class);
	    		// removes punctuation and gibberish 
	    		if (pos.length() == 1 || node.word().equals(pos)) {
	    			sg.removeVertex(node);
	    		}
	    		// removes stop words
	    		else if (removeStopwords && StopAnalyzer.ENGLISH_STOP_WORDS_SET.contains(node.word())) {
	    			sg.removeVertex(node);
	    		}
	    		// TODO: might be an interesting idea to remove all words that cannot be found in WordNET
	    	}
		}
		
	    public void writeSubtrees(ActionOutput actionOutput, String type, int wikiDocId, long sentenceHash, HashSet<ArrayList<IndexedWord>> arcs) throws Exception {
	    	String prefix = sentenceHash+"\t"+wikiDocId+"\t";
	    	for (ArrayList<IndexedWord> subtree : arcs) {
	    		StringBuilder pathString = new StringBuilder(prefix);
	    		int i = 0;
	    		for (IndexedWord node : subtree) {
	    			if (i>0)
	    				pathString.append(" ");
	    			pathString.append(node.word());
	    			pathString.append("/");
	    			pathString.append(node.get(CoreAnnotations.PartOfSpeechAnnotation.class));
	    			i++;
	    		}
	    		actionOutput.output(new TString(type), new TString(pathString.toString()));
	        }
	    }
	    
	    public void writeSentence(ActionOutput actionOutput, int wikiDocId, long sentenceHash, CoreMap sentence) throws Exception {
	    	List<CoreLabel> words = sentence.get(CoreAnnotations.TokensAnnotation.class);
	    	
	    	StringBuilder sentenceString = new StringBuilder(sentenceHash+"\t"+wikiDocId+"\t");
	    	for (int i=0; i< words.size(); i++) {
	    		CoreLabel word = words.get(i);
	    		// remove punctuation
	    		String pos = word.get(CoreAnnotations.PartOfSpeechAnnotation.class);
	    		if (pos.length() == 1 || word.word().equals(pos))
	    			continue;
	    		if (i>0)
	    			sentenceString.append(" ");
	    		sentenceString.append(word.word());
	    		sentenceString.append("/");
	    		sentenceString.append(word.get(CoreAnnotations.PartOfSpeechAnnotation.class));
            }
	    	actionOutput.output(new TString("sentence"), new TString(sentenceString.toString()));
	    }
	   
	    
	    public HashSet<ArrayList<IndexedWord>> allSubtrees (SemanticGraph sg, int treeSize) {
	    	/* holds mapping between nodes and their parents, used to navigate upwards in the tree
	    	 * HashMap is good enough (instead of TreeMap) because we just get elements based on SemanticGraph indexes of nodes
	    	 * These indices are already assigned according to post-order DFS traversal.
	    	 * We save this mapping separately because it is more efficient than getting the parents from the SemanticGraph directly.
	    	 */
	    	HashMap<IndexedWord,IndexedWord> parentMapping = new HashMap<IndexedWord, IndexedWord>();						
	    	HashSet<ArrayList<IndexedWord>> paths = new HashSet<ArrayList<IndexedWord>>();
	    	
	    	// list of nodes, sorted descending based on post-order DFS traversal
	    	List<IndexedWord> sortedNodes = sg.vertexListSorted();
	    	for (IndexedWord node : sortedNodes) {
	    		// save parent
	    		IndexedWord parent = sg.getParent(node);
	    		parentMapping.put(node, parent);
	    	}
	    	
	    	for (IndexedWord node : sortedNodes) {
	    		HashSet<ArrayList<IndexedWord>> incompletePaths = new HashSet<ArrayList<IndexedWord>>();
	    		discoverSubtrees(node, node.index(), incompletePaths, new HashSet<Integer>(), parentMapping, sg, treeSize);
	    		// save all subtrees of correct size and discard the rest
	    		for (ArrayList<IndexedWord> pathCandidate : incompletePaths) {
	    			if (pathCandidate.size() == treeSize)
	    				paths.add(pathCandidate);
	    		}
	    	}
	    	
	    	// DEBUG:
	    	//printPaths(paths, true);
	    	return paths;
	    }
	    
	    
	    public void printPaths (ArrayList<ArrayList<IndexedWord>> paths, boolean indexOnly) {
	    	for (ArrayList<IndexedWord> path : paths) {
	    		StringBuilder pathString = new StringBuilder();
	    		int i = 0;
	    		for (IndexedWord node : path) {
	    			if (i>0)
	    				pathString.append(" ");
	    			if (indexOnly)
	    				pathString.append(node.index());
	    			else
	    				pathString.append(node.word()+"/"+node.get(CoreAnnotations.PartOfSpeechAnnotation.class));
	    			i++;
	    		}
	    		log.info("Path: "+pathString.toString());
	    	}
	    }
	    
	    public void discoverSubtrees (IndexedWord focus, int refID, HashSet<ArrayList<IndexedWord>> incompletePaths, HashSet<Integer> seenIDs, 
	    		HashMap<IndexedWord,IndexedWord> parentMapping, SemanticGraph sg, int treeSize) {

	    	// make sure to initialize with one non-empty subtree containing current node
	    	if (incompletePaths.size() < 1) {
	    		ArrayList<IndexedWord> path = new ArrayList<IndexedWord>();
	    		path.add(focus);
	    		incompletePaths.add(path);
	    	}
	    	// add current node to seenIDs
	    	if (!seenIDs.contains(focus.index())) {
	    		seenIDs.add(focus.index());
	    	}
	    	// assume all paths have length 5
	    	boolean complete = true;
	    	// check paths completeness
	    	for (ArrayList<IndexedWord> path : incompletePaths) {
	    		if (path.size() < treeSize) {
	    			complete = false;
	    			break;
	    		}
	    	}
	    	// if all paths are complete or have seen all nodes then we have finished exploring the tree
	    	if (complete == true || seenIDs.size() == parentMapping.keySet().size())	
	    		return;

	    	// null focus node should not happen but double-check anyway
	    	if (focus == null) {
	    		log.info("null focus");
	    		return;
	    	}
	    	
	    	// first, iterate through all children, in order
	    	ArrayList<IndexedWord> children = (ArrayList)sg.getChildList(focus);	
	    	if (children != null) {
	    		for (IndexedWord child : children) {
	    			// skip children with lower index to prevent going backwards on branches already fully explored
	        		if (child.index() > refID) {	        			
						HashSet<ArrayList<IndexedWord>> pathClones = new HashSet<ArrayList<IndexedWord>>();
						// deep copy the subtree candidates to which we can still add nodes
						for (ArrayList<IndexedWord> clonePath : incompletePaths) {
							// skip complete trees, we cannot add any more nodes to them
							if (clonePath.size() < treeSize)
								// save a clone of the subtree to prevent overriding already existing objects
								pathClones.add((ArrayList<IndexedWord>)clonePath.clone());
						}
						
						// iterate through the incomplete candidates and try to add current child to them
						for (ArrayList<IndexedWord> path : pathClones) {
							// check that subtree does not already contain node to be added
							if (!path.contains(child)) {
								// check if the node is connected to any of the current nodes in the subtree
								boolean validRelationExists = false;
						
								ArrayList<IndexedWord> grandChildren = (ArrayList)sg.getChildList(child);
								for (IndexedWord pathNode : path) {
									// if subtree contains either parent (=focus) of child or children (=grandChildren) of child
									if (pathNode.index() == focus.index() || grandChildren.contains(pathNode)) {
										validRelationExists = true;
										break;
									}
								}
								
								if (validRelationExists) {		
									// add child to subtree
									path.add(child);
									seenIDs.add(child.index());
									// merge new subtree into candidate list
									incompletePaths.add(path);
								}							
							}
						}					
						// continue exploration
	        			discoverSubtrees(child, refID, incompletePaths, seenIDs, parentMapping, sg, treeSize);
	        		}
	        	}
	    	}
	    	
	    	// second, go up the tree through the parent
	    	IndexedWord parent = parentMapping.get(focus);
	    	// make sure the parent has not been added already to the subtree candidates
	    	if (parent != null && !seenIDs.contains(parent.index())) {
	    		seenIDs.add(parent.index());
	    		
	    		// add node to subtrees which still have space left
				for (ArrayList<IndexedWord> path : incompletePaths) {
					if (path.size() < treeSize) {
						path.add(parent);
					}
				}
	    		discoverSubtrees(parent, focus.index(), incompletePaths, seenIDs, parentMapping, sg, treeSize);
	    	}
	    }
	}

	public static Job createJob(String inDir, String outDir)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		// Read the input files
		ActionConf action = ActionFactory.getActionConf(ReadFromFiles.class);
		action.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(action);

		// extract subtrees
		actions.add(ActionFactory.getActionConf(Mapper.class));
		
		// Write the results on files
		action = ActionFactory.getActionConf(WriteToFiles.class);
		action.setParamString(WriteToFiles.S_PREFIX_FILE, "arcs");
		action.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(action);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: " + SubtreeExtractor.class.getSimpleName()
					+ " <input directory> <output directory>");
			System.exit(1);
		}
		
		// This is to remove the annoying StanfordNLP logging stuff
		try {
			PrintStream fserr = new PrintStream("/dev/null");
			System.setErr(fserr);		
		} 
		catch (Exception e) {		
		}

		// Start up the cluster
		Ajira ajira = new Ajira();
		try {
			ajira.startup();
		} catch(Throwable e) {
			log.error("Could not start up Ajira", e);
			System.exit(1);
		}

		// With this command we ensure that we submit the job only once
		if (ajira.amItheServer()) {

			// Configure the job and launch it!
			try {
				Job job = createJob(args[0], args[1]);
				Submission sub = ajira.waitForCompletion(job);
				sub.printStatistics();
				if (sub.getState().equals(Consts.STATE_FAILED)) {
					log.error("The job failed", sub.getException());
				}

			} catch (ActionNotConfiguredException e) {
				log.error("The job was not properly configured", e);
			} finally {
				ajira.shutdown();		// WARNING: shutdown fails on large data sets
			}
		}
	}

}
