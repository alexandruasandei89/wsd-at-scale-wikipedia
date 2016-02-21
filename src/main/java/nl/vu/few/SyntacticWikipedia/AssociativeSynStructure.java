package nl.vu.few.SyntacticWikipedia;

import java.util.ArrayList;
import java.util.StringTokenizer;

import edu.ucla.sspace.util.Pair;

public class AssociativeSynStructure {
	private ArrayList<Pair<String>> arc;
	private PosVector posVector;
	private boolean matchedSenseFocus;
	private boolean matchedSenseGeneric;
	
	AssociativeSynStructure(ArrayList<Pair<String>> arc, PosVector posVector) {
		this.arc = arc;
		this.posVector = posVector;
		this.matchedSenseFocus = false;
		this.matchedSenseGeneric = false;
	}
	
	public ArrayList<Pair<String>> getArc() {
		return this.arc;
	}
	
	public PosVector getPos() { 
		return this.posVector;
	}
	
	public void setFocusMatch() {
		this.matchedSenseFocus = true;
	}
	
	public void setGenericMatch() {
		this.matchedSenseGeneric = true;
	}
	
	public boolean isFocusMatch() {
		return this.matchedSenseFocus;
	}
	
	public boolean isGenericMatch() {
		return this.matchedSenseGeneric;
	}
	
	public boolean isMatch() {
		return this.matchedSenseGeneric || this.matchedSenseFocus;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		StringTokenizer pos_tokenizer = new StringTokenizer(posVector.getOccurences(), " ");
		for (int i=0; i<arc.size(); i++) {
			Pair<String> element = arc.get(i);
			if (i>0)
				sb.append(" ");
			sb.append(element.x);
			sb.append("/");
			sb.append(pos_tokenizer.nextToken());
		}
		
		return sb.toString();
	}
}
