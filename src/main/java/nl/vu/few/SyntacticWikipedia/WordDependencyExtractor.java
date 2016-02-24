package nl.vu.few.SyntacticWikipedia;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

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
import edu.ucla.sspace.util.Pair;
import gov.llnl.ontology.wordnet.OntologyReader;
import gov.llnl.ontology.wordnet.Synset;
import gov.llnl.ontology.wordnet.WordNetCorpusReader;


public class WordDependencyExtractor {
	
	static final Logger log = LoggerFactory.getLogger(WordDependencyExtractor.class);

	public static class Mapper extends Action {
		private FNV164 hasher; // very fast collision-rare 64bit hash (FNV1 implementation)
		private Set<?> stopWords;
		
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
			props.put("annotators", "tokenize,ssplit,pos,depparse,lemma");
			// auxiliary variables
			sumPreproctime = 0;
			nsentences = ndocs = 0;
			this.stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
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
	        time = System.currentTimeMillis() - time;
	        context.incrCounter("SUBTREE_BUILD_TIME", time);
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
	    	String prefix = treeName+"\t"+wikiDocId+"\t";
	    	for (ArrayList<IndexedWord> subtree : arcs) {
	    		StringBuilder pathString = new StringBuilder(prefix);
	    		int i = 0;
	    		for (IndexedWord node : subtree) {
	    			if (i>0)
	    				pathString.append(" ");
	    			pathString.append(node.get(CoreAnnotations.LemmaAnnotation.class));
	    			pathString.append("/");
	    			pathString.append(node.get(CoreAnnotations.PartOfSpeechAnnotation.class));
	    			i++;
	    		}
	    		actionOutput.output(new TLong(sentenceHash), new TString(pathString.toString()));
	        }
	    }
	    
	    public void writeSentence(ActionOutput actionOutput, int wikiDocId, long sentenceHash, CoreMap sentence) throws Exception {
	    	List<CoreLabel> words = sentence.get(CoreAnnotations.TokensAnnotation.class);
	    	
	    	StringBuilder sentenceString = new StringBuilder("sentence\t"+wikiDocId+"\t");
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
	    	actionOutput.output(new TLong(sentenceHash), new TString(sentenceString.toString()));
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
	    		//log.info("node "+node.index());
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
	
	
	public static class Reducer extends Action {
		private OntologyReader wordnet;
		private PersonalizedPageRank disambiguator;
		
		private long sentenceHash;
		private long wikiDocID;
		private String type;
		
		// TIMERS (in ms)
        private long time_preprocessing = 0;
        private long time_disambiguation = 0;
        private long time_postprocessing = 0;
        private long time_ticker = 0;
		
		@Override
		public void startProcess(ActionContext context) throws Exception {
			super.startProcess(context);
			// initialize wordnet
	        this.wordnet = WordNetCorpusReader.initialize("data/dict", true);
	        // initialize WSD graph
	        this.disambiguator = new PersonalizedPageRank(this.wordnet);
	        this.disambiguator.preprocess();
		}
		
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {			
			sentenceHash = ((TLong) tuple.get(0)).getValue();
			TBag values = (TBag) tuple.get(1);
			time_ticker = System.currentTimeMillis();
			for (Tuple t : values) {
				int skipIndex = 2; 	// start with one to account for the extra tabs
				TString val = (TString) t.get(0);
				StringTokenizer keyvalTok = new StringTokenizer(val.toString(), "\t");
				type = keyvalTok.nextToken();
				skipIndex += type.length();
				String aux = keyvalTok.nextToken();
				skipIndex += aux.length();
				wikiDocID = Long.parseLong(aux);
				String arcs = val.toString().substring(skipIndex);
				
				// split in words
				StringTokenizer word_splitter = new StringTokenizer(arcs.toString(), " ");
				int tokenCount = word_splitter.countTokens();
				// make sure to skip long sentences
				if (tokenCount > 40) {
					continue;
				}
				// disambiguate input text (sentence or arc)
				ArrayList<WordAmbiguity> ambiguous_input = new ArrayList<WordAmbiguity>();
				
				StringBuilder disambiguation_MFS = new StringBuilder();		// output containing Most Frequent Sense for each word
				disambiguation_MFS.append(type+"\tMFS\t");
				
				int i = 0;
				while (word_splitter.hasMoreElements()) {
					String word = new String();
					String pos = new String();
					String wordCandidate = word_splitter.nextToken();
					StringTokenizer adnotated_word = new StringTokenizer(wordCandidate, "/");
					if (adnotated_word.countTokens() < 2) {
						word = wordCandidate;
					}
					else {
						word = adnotated_word.nextToken();
						if (adnotated_word.hasMoreElements())
							pos = adnotated_word.nextToken();									// Penn Treebank tagset
					}
					String wordLowercase = word.toLowerCase();
					            	
	            	// disambiguate based on POS
	            	Synset[] senses = null;
	            	if (pos.isEmpty()) {
	            		senses = wordnet.getSynsets(word);
	            	}
	            	else {
	            		senses = wordnet.getSynsets(word, PennTreebankTag.TagToWordnet(pos)); 
	    				// disambiguate using any POS
	    				if (senses.length < 1) {
	    					senses = wordnet.getSynsets(word);
	    				}
	            	}
					// add to list for PPR disambiguation
					ambiguous_input.add(new WordAmbiguity(word, new ArrayList<Synset>(Arrays.asList(senses)), pos));
					// construct MFS output
					// NOTE: we only do this for the sentence, because that's were all the words show up and we only need MFS once per word
					
					if (type.equals("sentence")) {
						if (i>0)
							disambiguation_MFS.append(" ");
						disambiguation_MFS.append(word);
						disambiguation_MFS.append("/");
						if (senses.length > 0)
							disambiguation_MFS.append(senses[0].getName());
						disambiguation_MFS.append("/");
						disambiguation_MFS.append(pos);
						disambiguation_MFS.append("/");
						disambiguation_MFS.append(senses.length);
						i++;
					}		
				}
				if (ambiguous_input.size() < 5) {
					//log.info("ERROR, too small: ["+type+"] "+arcs);
					return;
				}
				switch (type) {
					case "sentence":
						context.incrCounter("SENTENCES_SEEN", 1);
						context.incrCounter("WORDS_SEEN_IN_SENTENCE", tokenCount);
						break;
					case "quadarcs":
					case "triarcs":
						context.incrCounter("ARCS_SEEN", 1);
						context.incrCounter("WORDS_SEEN_IN_ARCS", tokenCount);
						break;
				}
				time_preprocessing = System.currentTimeMillis() - time_ticker;
				
		        // perform PPR disambiguation
				time_ticker = System.currentTimeMillis();
		        disambiguator.processNGram(ambiguous_input);	// modifies the ngram contents to set the correct sense
		        time_disambiguation = System.currentTimeMillis() - time_ticker;
		        // output disambiguation
		        time_ticker = System.currentTimeMillis();
		        StringBuilder disambiguation_PPR = new StringBuilder();
		        disambiguation_PPR.append(type+"\tPPR\t");
		        for (i=0; i<ambiguous_input.size(); i++) {
		        	if (type.equals("sentence") && ambiguous_input.get(i).getPolysemy() == 1)
		        		context.incrCounter("NON_AMBIGUOUS_WORDS", 1);
		        	String word = ambiguous_input.get(i).getWord();
		        	Synset sense = ambiguous_input.get(i).getRealSense();
		        	String pos = ambiguous_input.get(i).getPos();
		        	if (i>0)
		        		disambiguation_PPR.append(" ");
		        	disambiguation_PPR.append(word);
		        	disambiguation_PPR.append("/");
		        	if (sense != null)
		        		disambiguation_PPR.append(sense.getName());
		        	else {
		        		disambiguation_PPR.append("-");
		        		context.incrCounter("SENSE_MISSING", 1);
		        	}
		        	disambiguation_PPR.append("/");
		        	disambiguation_PPR.append(pos);
		        	disambiguation_PPR.append("/");
		        	disambiguation_PPR.append(ambiguous_input.get(i).getPolysemy());
		        }
		        // output PPR
		     	actionOutput.output(new TLong(sentenceHash), new TString(disambiguation_PPR.toString())); 	
		     	
		     	if (type.equals("sentence")) {
		     		// output MFS
			     	actionOutput.output(new TLong(sentenceHash), new TString(disambiguation_MFS.toString())); 
		         	// output ngrams PPR
		         	//getAllNgrams(11, sentenceHash, ambiguous_input, context, actionOutput);
		         	
		     		context.incrCounter("DISAMBIGUATION_TIME_SENTENCE", time_disambiguation);
		     	}
		     	else
		     		context.incrCounter("DISAMBIGUATION_TIME_ARCS", time_disambiguation);
		     	time_postprocessing = System.currentTimeMillis() - time_ticker;
		     	
		     	context.incrCounter("PREPROCESSING_TIME", time_preprocessing);
		     	context.incrCounter("POSTPROCESSING_TIME", time_postprocessing);
			}
		}
		
		private void getAllNgrams(int size, long sentenceHash, ArrayList<WordAmbiguity> sentence, ActionContext context, ActionOutput actionOutput) throws Exception {
			if (sentence.size() < size)
				return;
			for (int i = 0; i<sentence.size()-size; i++) {
				StringBuilder ngram = new StringBuilder();
				ngram.append("ngram\tPPR\t");
				for (int j=i; j<i+size; j++) {
					context.incrCounter("WORDS_SEEN_IN_NGRAMS", 1);
					String word = sentence.get(j).getWord();
		        	Synset sense = sentence.get(j).getRealSense();
		        	String pos = sentence.get(j).getPos();
		        	if (j>0) {
		        		ngram.append(" ");
		        	}
		        	ngram.append(word);
		        	ngram.append("/");
		        	if (sense != null)
		        		ngram.append(sense.getName());
		        	else
		        		ngram.append("-");
		        	ngram.append("/");
		        	ngram.append(pos);
		        	ngram.append("/");
		        	ngram.append(sentence.get(j).getPolysemy());
				}
				actionOutput.output(new TLong(sentenceHash), new TString(ngram.toString())); 
			}
		}
	}
	
	public static class Analyser extends Action {
		private long sentenceHash;
		private String type;
		
		
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {			
			sentenceHash = ((TLong) tuple.get(0)).getValue();
			TBag values = (TBag) tuple.get(1);

			ArrayList<String> sentence_words = new ArrayList<String>();
			ArrayList<String> sentence_PPR = new ArrayList<String>();
			ArrayList<String> sentence_MFS = new ArrayList<String>();
			StringBuilder originalStringSentence = new StringBuilder();
			ArrayList<AssociativeSynStructure> arcs = new ArrayList<AssociativeSynStructure>();
	        
			for (Tuple t : values) {
				int skipIndex = 1; 	// start with one to account for the extra tab between type and wikiDocID
				TString val = (TString) t.get(0);
				StringTokenizer inputTokenizer = new StringTokenizer(val.toString(), "\t");
				if (inputTokenizer.countTokens() < 3) {
					//log.info("Error parsing: "+val.toString());
					continue;
				}
				type = inputTokenizer.nextToken();
				String algType = inputTokenizer.nextToken();
				String disambiguation = inputTokenizer.nextToken();
				StringTokenizer wordsTokenizer = new StringTokenizer(disambiguation, " ");

				ArrayList<Pair<String>> arc = new ArrayList<Pair<String>>();
				
				PosVector syntacticVector = new PosVector();
				while (wordsTokenizer.hasMoreTokens()) {
					String candidate = wordsTokenizer.nextToken();
					StringTokenizer senseTokenizer = new StringTokenizer(candidate, "/");
					String word = senseTokenizer.nextToken();
					String senseName = senseTokenizer.nextToken();
					String pos = senseTokenizer.nextToken();
					
					switch (type) {
						case "sentence":
							if (originalStringSentence.length() > 0)
								originalStringSentence.append(" ");
							originalStringSentence.append(word);
							
							if (algType.equals("PPR")) {
								sentence_PPR.add(senseName);
								
							}
							else {
								sentence_MFS.add(senseName);
								sentence_words.add(word);
							}
							break;
						case "quadarcs":
						case "triarcs":
							arc.add(new Pair(word, senseName));
							syntacticVector.setPos(pos);
							break;
					}
					
				}
				if (!type.equals("sentence") && arc.size() < 5) {
					//log.info("ERROR, too small: ["+type+"] "+disambiguation);
					continue;
				}
				else if (arc.size() > 0)
					arcs.add(new AssociativeSynStructure(arc, syntacticVector)); 				
			}
			
			// check where senses match
			for (int i=0; i<sentence_words.size(); i++) {
				for (AssociativeSynStructure synStruct : arcs) {
					
					ArrayList<Pair<String>> arc = synStruct.getArc();
					if (arc == null || arc.size() == 0 || sentence_PPR.get(i) == null || sentence_PPR.get(i).length() < 2)
		        		continue;
					if (isSameSense(arc, sentence_words.get(i), sentence_PPR.get(i)) > -1) {
						StringBuilder keyOutput = new StringBuilder();
						keyOutput.append(sentence_words.get(i));
						keyOutput.append(" ");
						keyOutput.append(sentence_PPR.get(i));
						actionOutput.output(new TString(keyOutput.toString()), new TString(synStruct.toString()));
					}			
				}			
			}
			
		}
		
		private boolean isTrue (boolean[] vector) {
			for (boolean v:vector) {
				if (v==true)
					return true;
			}
			return false;
		}
		
		private int isSameSense (ArrayList<Pair<String>> arc, String sentence_word, String sentence_sense) {
			for (int i=0; i<arc.size(); i++) {
				Pair<String> word = arc.get(i);
				if (word.x.equals(sentence_word) && word.y.equals(sentence_sense)) {
					return i;
				}
			}
			return -1;
		}
		
		private boolean isSameSense (String word, String senseName, ArrayList<String> wordReferences, ArrayList<String> senseReferences, ActionContext context) {
			for (int i=0; i<wordReferences.size(); i++) {
				if (wordReferences.get(i).equals(word)) {
					if (senseReferences.get(i).equals(senseName))
						return true;
					else{
						return false;
					}
				}
			}
			context.incrCounter("WORD_NOT_FOUND", 1);
			return false;
		}
		
		private boolean isSameOrder (ArrayList<Pair<String>> arc, ArrayList<String> wordReferences) {
			int prevIndex = -1;
			for (Pair<String> wordSensePair : arc) {
				int currentIndex = wordReferences.indexOf(wordSensePair.x);
				if (currentIndex < prevIndex)
					return false;
				prevIndex = currentIndex;
			}
			return true;
		}
		
		private String collapse (ArrayList<Pair<String>> wordReferences) {
			StringBuilder collapsed = new StringBuilder();
			for (Pair<String> token : wordReferences) {
				if (collapsed.length() > 0)
					collapsed.append(" ");
				collapsed.append(token.x);
			}
			return collapsed.toString();
		}
		
		private String collapseSynset (ArrayList<Pair<String>> wordReferences) {
			StringBuilder collapsed = new StringBuilder();
			for (Pair<String> token : wordReferences) {
				if (collapsed.length() > 0)
					collapsed.append(" ");
				collapsed.append(token.y);
			}
			return collapsed.toString();
		}
		
		private void getAllNgrams(int size, long sentenceHash, ArrayList<WordAmbiguity> sentence, ActionContext context, ActionOutput actionOutput) throws Exception {
			if (sentence.size() < size)
				return;
			for (int i = 0; i<sentence.size()-size; i++) {
				StringBuilder ngram = new StringBuilder();
				ngram.append("ngram\tPPR\t");
				for (int j=i; j<i+size; j++) {
					context.incrCounter("WORDS_SEEN_IN_NGRAMS", 1);
					String word = sentence.get(j).getWord();
		        	Synset sense = sentence.get(j).getRealSense();
		        	String pos = sentence.get(j).getPos();
		        	if (i>0) {
		        		ngram.append(" ");
		        	}
		        	ngram.append(word);
		        	ngram.append("/");
		        	if (sense != null)
		        		ngram.append(sense.getName());
		        	else
		        		ngram.append("-");
		        	ngram.append("/");
		        	ngram.append(pos);
		        	ngram.append("/");
		        	ngram.append(sentence.get(j).getPolysemy());
				}
				actionOutput.output(new TLong(sentenceHash), new TString(ngram.toString())); 
			}
		}
	}
	
	public static class GroupByWordSense extends Action {
		private String wordSenseKey;
		private HashMap<String, Long> groupedByFrequency;
		
		@Override
		public void startProcess(ActionContext context) throws Exception {
			super.startProcess(context);
			this.groupedByFrequency = new HashMap<String,Long>();
		}
		
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {			
			wordSenseKey = ((TString) tuple.get(0)).getValue();
			TBag values = (TBag) tuple.get(1);
        
			for (Tuple t : values) {
				TString val = (TString) t.get(0);
				add(val.toString());
			}
			
			long highestFrequency = -1;
			String highestFrequencyKey = "";
			Iterator it = groupedByFrequency.entrySet().iterator();
			while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        
		        if ((Long)pair.getValue() > highestFrequency) {
		        	highestFrequency = (Long)pair.getValue();
		        	highestFrequencyKey = pair.getKey().toString();
		        }
		    }
			
			actionOutput.output(new TString(wordSenseKey), new TString(highestFrequencyKey));
		}
		
		private void add( String element  ) { 
	        if( !groupedByFrequency.containsKey( element ) ){
	        	groupedByFrequency.put( element, 1L );
	        } else { 
	        	groupedByFrequency.put( element, groupedByFrequency.get( element ) + 1L );
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
		
		// Groups the pairs
		action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TLong.class.getName(), TString.class.getName());
		action.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		actions.add(action);

		// reduce
		actions.add(ActionFactory.getActionConf(Reducer.class));
		
		// Groups the pairs
		action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TLong.class.getName(), TString.class.getName());
		action.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		actions.add(action);
		
		// analyse
		actions.add(ActionFactory.getActionConf(Analyser.class));
		
		// Groups the pairs
		action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TString.class.getName(), TString.class.getName());
		action.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		actions.add(action);
		
		// analyse
		actions.add(ActionFactory.getActionConf(GroupByWordSense.class));
		
		// Write the results on files
		action = ActionFactory.getActionConf(WriteToFiles.class);
		action.setParamString(WriteToFiles.S_PREFIX_FILE, "wordsense");
		action.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(action);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: " + WordDependencyExtractor.class.getSimpleName()
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
				ajira.shutdown();
			}
		}
	}

}
