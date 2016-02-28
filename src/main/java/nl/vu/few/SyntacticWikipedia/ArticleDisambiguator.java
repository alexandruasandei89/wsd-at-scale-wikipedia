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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.lucene.analysis.core.StopAnalyzer;
import org.getopt.util.hash.FNV164;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import gov.llnl.ontology.wordnet.OntologyReader;
import gov.llnl.ontology.wordnet.Synset;
import gov.llnl.ontology.wordnet.WordNetCorpusReader;

/*
 * This class takes Wikipedia plaintext as input (one per line, with wikipedia document ID as key)
 * and creates annotated sentences. Used to produce the wsd-wikipedia-articles data set. 
 */

public class ArticleDisambiguator {
	
	static final Logger log = LoggerFactory.getLogger(ArticleDisambiguator.class);

	public static class Mapper extends Action {
		private FNV164 hasher; // very fast collision-rare 64bit hash (FNV1 implementation)
		private OntologyReader wordnet;
		private PersonalizedPageRank disambiguator;
		private Set<?> stopWords;
		
		Properties props;
		long sumPreproctime;
		long ndocs;
		long nsentences;
		
		
		@Override
		public void startProcess(ActionContext context) throws Exception {
			super.startProcess(context);
			// configure CoreNLP pipeline
			props = new Properties();
			props.put("annotators", "tokenize,ssplit,pos,depparse,lemma");
			sumPreproctime = 0;
			nsentences = ndocs = 0;
			// initialize wordnet
	        this.wordnet = WordNetCorpusReader.initialize("data/dict", true);
	        this.stopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
	        // initialize WSD graph
	        this.disambiguator = new PersonalizedPageRank(this.wordnet);
	        this.disambiguator.preprocess();
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
			
			context.incrCounter("ARTICLES_SEEN", 1);
			// init CoreNLP pipeline
			Annotation annotation = new Annotation(documentText);
	        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
	        // start CoreNLP pipeline
	        pipeline.annotate(annotation);
	        // get sentences from CoreNLP pipeline (created by ssplit annotator)
	        List<CoreMap> sentences =  annotation.get(CoreAnnotations.SentencesAnnotation.class);
	        
	        if (sentences != null) {
	        	for (CoreMap sentence : sentences) {	 
	        		context.incrCounter("SENTENCES_SEEN", 1);
	                // compute sentence hash
	                String originalSentence = sentence.get(CoreAnnotations.TextAnnotation.class);
	                hasher.update(originalSentence);
	                long sentenceHash = hasher.getHash();
	                // skip very long sentences
	                if (sentence.size() > 40) {
	        			//actionOutput.output(new TString("excluded"), new TString(sentenceHash+"\t"+wikiDocId+"\t"+originalSentence));
	                	continue;
	        		}
	                // get syntactic parse tree from CoreNLP pipeline (created by depparse annotator)
	                SemanticGraph sg = sentence.get(SemanticGraphCoreAnnotations.BasicDependenciesAnnotation.class);
	                // skip very long sentences
	                if (sg.vertexSet().size() > 40) {
	        			//actionOutput.output(new TString("excluded"), new TString(sentenceHash+"\t"+wikiDocId+"\t"+originalSentence));
	                	continue;
	        		}
	                // remove punctuation, gibberish (i.e. malformed HTML tags) and stopwords
	                removeUndesired(sg, true);
	                // get all nodes from syntactic parse tree
	                List<IndexedWord> sortedNodes = sg.vertexListSorted();
	                ArrayList<WordAmbiguity> ambiguous_input = new ArrayList<WordAmbiguity>();
	                for (IndexedWord node : sortedNodes) {
	                	Synset[] senses = null;
	                	String pos_tag = node.get(CoreAnnotations.PartOfSpeechAnnotation.class);
	                	String base_word = node.lemma(); 	// equivalent to node.get(CoreAnnotations.LemmaAnnotation.class)
	                	// get synsets from WordNET
	                	if (pos_tag.isEmpty()) {
	                		senses = wordnet.getSynsets(base_word);
	                	}
	                	else {
	                		senses = wordnet.getSynsets(base_word, PennTreebankTag.TagToWordnet(pos_tag)); 
	        				// disambiguate using any POS
	        				if (senses.length < 1) {
	        					senses = wordnet.getSynsets(base_word);
	        				}
	                	}
	                	// save information in required data structure
	                	WordAmbiguity ambiguous_token = new WordAmbiguity(node.word(), new ArrayList<Synset>(Arrays.asList(senses)), pos_tag);
	                	IndexedWord parent = sg.getParent(node);
	                	
	                	if (parent != null)
	                		ambiguous_token.setParent(parent.index());
	                	else
	                		ambiguous_token.setParent(0);
	                	ambiguous_input.add(ambiguous_token);
	                }
	                // perform disambiguation (modifies the ambiguous_input to set the correct sense)
	                disambiguator.processNGram(ambiguous_input);	
	                // prepare output
	                StringBuilder disambiguation = new StringBuilder();
	                StringBuilder prefix = new StringBuilder();
	                prefix.append(sentenceHash);
	                prefix.append("\t");
	                prefix.append(wikiDocId);
	                prefix.append("\t");
	                
	                for (WordAmbiguity disambiguated_word : ambiguous_input) {
	                	if (disambiguation.length() > 0)
	                		disambiguation.append(" ");
	                	Synset sense = disambiguated_word.getRealSense();
	                	
	                	disambiguation.append(disambiguated_word.getWord());
	                	disambiguation.append("/");
	                	disambiguation.append(disambiguated_word.getPos());
	                	disambiguation.append("/");
	                	disambiguation.append(disambiguated_word.getParent());
	                	disambiguation.append("/");
	                	if (sense != null)
	                		disambiguation.append(sense.getName());
	                	
	                }
	                actionOutput.output(new TString(prefix.toString()), new TString(disambiguation.toString()));
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
		
		public void writeAnnotatedSentence(ActionOutput actionOutput, SemanticGraph sg, int wikiDocId, long sentenceHash, List<IndexedWord> vertexList) throws Exception {
	    	String prefix = sentenceHash+"\t"+wikiDocId+"\t";
    		StringBuilder pathString = new StringBuilder();
    		int i = 0;
    		for (IndexedWord node : vertexList) {
    			IndexedWord parent = sg.getParent(node);
    			if (i>0)
    				pathString.append(" ");
    			pathString.append(node.word());
    			pathString.append("/");
    			if (parent == null)
    				pathString.append("ROOT");
    			else
    				pathString.append(sg.getParent(node).index());
    			pathString.append("/");
    			pathString.append(node.get(CoreAnnotations.PartOfSpeechAnnotation.class));
    			i++;
    		}
    		actionOutput.output(new TString(prefix), new TString(pathString.toString()));
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
		action.setParamString(WriteToFiles.S_PREFIX_FILE, "annotated");
		action.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(action);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: " + ArticleDisambiguator.class.getSimpleName()
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
				ajira.shutdown();	// WARNING: shutdown fails on large data sets
			}
		}
	}

}
