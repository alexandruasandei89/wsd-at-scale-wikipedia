package nl.vu.few.SyntacticWikipedia;

import gov.llnl.ontology.wordnet.Synset;
import java.util.ArrayList;

public class WordAmbiguity {
	private String word;
	private String pos;
	private int indexOfParent;
	private String relationToParent;
	private ArrayList<Synset> synsets;	// ordered by frequency, with the first being the most frequent
	private Synset realSense;
	private int mapIndex;
	
	public WordAmbiguity(String word) {
		this.word = word;
		this.mapIndex = -1;
		this.synsets = new ArrayList<Synset>();
		this.realSense = null;
	}

	public WordAmbiguity(String word, ArrayList<Synset> synsets) {
		this(word);
		this.synsets = synsets;
	}
	
	public WordAmbiguity(String word, ArrayList<Synset> synsets, String pos) {
		this(word, synsets);
		this.pos = pos;
	}

	
	public void addSynsets(Synset[] new_synsets) {
		for (int i=0; i<new_synsets.length; i++) {
			this.synsets.add(new_synsets[i]);
		}
	}
	
	public void setParent(int indexOfParent) {
		this.indexOfParent = indexOfParent;
	}
	
	public int getParent() {
		return this.indexOfParent;
	}
	
	public void setParentRelation(String relationToParent) {
		this.relationToParent = relationToParent;
	}
	
	public String getParentRelation() {
		return this.relationToParent;
	}
	
	public void setPos (String pos) {
		this.pos = pos;
	}
	
	public String getPos () {
		return this.pos;
	}
	
	public void setMapIndex(int map_index) {
		this.mapIndex = map_index;
	}
	
	public int getMapIndex() {
		return this.mapIndex;
	}
	
	public int getPolysemy() {
		return synsets.size();
	}
	
	public void setRealSense(Synset sense) {
		this.realSense = sense;
	}
	
	public Synset getRealSense() {
		return this.realSense;
	}
	
	public Synset getMostFrequentSense() {
		return this.synsets.get(0);
	}
	
	public String getWord() {
		return word;
	}
	
	public ArrayList<Synset> getSynsets() {
		return synsets;
	}
	
	private int countRelations() {
		int relations = 0;
		for (int i=0; i<synsets.size(); i++) {
			relations += synsets.get(i).getNumRelations();
		}
		return relations;
	}
	
}
