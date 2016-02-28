package nl.vu.few.SyntacticWikipedia;

import gov.llnl.ontology.util.ExtendedList;
import gov.llnl.ontology.util.ExtendedMap;
import gov.llnl.ontology.wordnet.BaseSynset;
import gov.llnl.ontology.wordnet.OntologyReader;
import gov.llnl.ontology.wordnet.Synset;
import gov.llnl.ontology.wordnet.Synset.PartsOfSpeech;
import gov.llnl.ontology.wordnet.SynsetPagerank;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.ucla.sspace.vector.CompactSparseVector;
import edu.ucla.sspace.vector.SparseDoubleVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * This class performs pagerank over each word in a sentence by creating a fake
 * synset that connects to it's possible target sense in the wordnet
 * graph.  Context words are also given a pseudo synset.  The algorithm
 * then runs the standard PageRank algorithm over this extended graph centering
 * the initial probabilities over the pseudo synset to bias the system
 * towards flowing information from those nodes in the graph. The target synset
 * with the highest page rank for each word will be selected as the correct sense.
 *
 * It modifies the synsets in the wordnet graph ones with a link tag during initialization. 
 * Afterwords, no changes are made to the graph structure.
 */
public class PersonalizedPageRank {

    // the synset relation name for links created in the page rank graph
    public static final String FAKE_LINK_RELATION = "related";
    private OntologyReader wordnet;
    // mapping from synsetss to unique indexes
    private Map<Synset, Integer> synsetMap;
    // the original synsets in the wordnet graph.
    private List<Synset> synsetList;

    public PersonalizedPageRank(OntologyReader wordnet) {
        this.wordnet = wordnet;
        /* Create the list of synsets that should serve as the base graph during
         * Word Sense Dismabiguation.  This requires creating the list of
         * synsets and a mapping from each synset to it's index in the list
         */
        synsetMap = Maps.newHashMap();
        synsetList = Lists.newArrayList();
        for (String lemma : wordnet.wordnetTerms()) {
            for (Synset synset : wordnet.getSynsets(lemma)) {
                if (!synsetMap.containsKey(synset)) {
                    synsetMap.put(synset, synsetMap.size());
                    synsetList.add(synset);
                }
            }
        }
        SynsetPagerank.setupTransitionAttributes(synsetList, synsetMap);
    }

    /*
     * Newly introduced method, not in the original PPR implementation.
     */
    protected void preprocess() {
        // run the page rank algorithm over the default graph
        SynsetPagerank.setupTransitionAttributes(synsetList, synsetMap);
        // use c=0.85 and 5 iterations as the default parameters
        SparseDoubleVector pageRanks = SynsetPagerank.computePageRank(synsetList, new CompactSparseVector(), .85, 5);     
    }
    
    /* Iterates through the ambiguous_ngram and sets the correct word sense
     * after performing PPR on the enriched graph
     */
    protected void processNGram(ArrayList<WordAmbiguity> ambiguous_ngram) {
        Map<Synset, Integer> localMap = new ExtendedMap<Synset, Integer>(synsetMap);
        List<Synset> localList = new ExtendedList<Synset>(synsetList);

        // add the artificial synsets for each of the context words.
        ArrayList<Integer> contentIndexes = new ArrayList<Integer>();
        for (WordAmbiguity contentWord : ambiguous_ngram) {
            int termIndex = addTermNode(localList, localMap, contentWord.getSynsets());
            contentWord.setMapIndex(termIndex);
            contentIndexes.add(termIndex);
        }

        // place an even random surfer probability on each artificial synset
        double numTerms = localList.size() - synsetList.size();
        SparseDoubleVector sourceWeights = new CompactSparseVector();
        for (int i = synsetList.size(); i < localList.size(); ++i)
            sourceWeights.set(i, 1d/numTerms);
        // run the page rank algorithm over the created graph.
        SynsetPagerank.setupTransitionAttributesLimited(localList, localMap, synsetList.size());
        // the classic method, without calling preprocess(), should use setupTransitionAttributes()

        // use c=0.85 and 20 iterations as the default parameters
        SparseDoubleVector pageRanks = SynsetPagerank.computePageRank(localList, sourceWeights, .85, 20);
        // determine the best sense for each ambiguous word
        for (WordAmbiguity contentWord : ambiguous_ngram) {
        	int focusIndex = contentWord.getMapIndex();
            Synset maxSynset = null;
            double maxRank = 0;
            for (Synset related : localList.get(focusIndex).getRelations(FAKE_LINK_RELATION)) {
                int index = localMap.get(related);
                double rank = pageRanks.get(index);
                if (maxRank <= rank) {
                    maxRank = rank;
                    maxSynset = related;
                }
            }

            // save the chosen word sense
            contentWord.setRealSense(maxSynset);
        }
        
    }

    /**
     * adds a new artificial synset corresponding to the word.  This new synset
     * will be connected to each of it's possible word senses via a fake "related" 
     * link.  Returns the index in the localMap if the word was added and -1 otherwise
     */
    private int addTermNode(List<Synset> localList,
                            Map<Synset, Integer> localMap, 
                            ArrayList<Synset> synsets) {
    	int termIndex = -1;
        // create a link for each artificial synset to the word's possible
        // senses.  Note that the part of speech doesn't matter for these nodes.
        BaseSynset termSynset = new BaseSynset(PartsOfSpeech.NOUN);
        for (Synset possibleSense : synsets) {
            termSynset.addRelation(FAKE_LINK_RELATION, possibleSense);
        }

        // Add the word to the list of known synsets.
        localList.add(termSynset);
        termIndex = localMap.size();
        localMap.put(termSynset, localMap.size());
        return termIndex;
    }

}
