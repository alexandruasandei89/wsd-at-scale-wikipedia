package nl.vu.few.SyntacticWikipedia;

import java.util.Arrays;
import java.util.StringTokenizer;

public class PosVector implements Comparable {
	long[] vector = new long[16];
	StringBuilder posOccurence;
	/*
	 * @see PennTreebankTag
	 * the positions are for supported tags from 0 to 15:
	JJ => 0
	JJR => 1
	JJS => 2
	NN => 3
	NNS => 4
	NNP => 5
	NNPS => 6
	RB => 7
	RBR => 8
	RBS => 9
	VB => 10
	VBD => 11
	VBG => 12
	VBN => 13
	VBP => 14
	VBZ => 15
	 */
	
	PosVector() {
		Arrays.fill(this.vector, 0);
		posOccurence = new StringBuilder();
	}
	
	PosVector(String posVectorString) {
		this();
		
		StringTokenizer tokens = new StringTokenizer(posVectorString, " ");
		while (tokens.hasMoreTokens()) {
			String posToken = tokens.nextToken();
			this.setPos(posToken);
		}
	}
	
	PosVector(String posVectorString, boolean flag) {
		this();
		
		StringTokenizer tokens = new StringTokenizer(posVectorString, "-");
		while (tokens.hasMoreTokens()) {
			String posToken = tokens.nextToken();
			StringTokenizer intermediate = new StringTokenizer(posToken, "#");
			String pos = intermediate.nextToken();
			int count = Integer.parseInt(intermediate.nextToken());
			for (int i=0; i<count; i++)
				this.setPos(pos);
		}
	}
	
	public int getIndex(String pos ) {
		switch(pos){
			case "JJ": return 0;
			case "JJR": return 1;
			case "JJS": return 2;
			case "NN": return 3;
			case "NNS": return 4;
			case "NNP": return 5;
			case "NNPS": return 6;
			case "RB": return 7;
			case "RBR": return 8;
			case "RBS": return 9;
			case "VB": return 10;
			case "VBD": return 11;
			case "VBG": return 12;
			case "VBN": return 13;
			case "VBP": return 14;
			case "VBZ": return 15;
			default: return -1;
		}
	}
	
	public String getPos(int index) {
		switch(index){
			case 0: return "JJ";
			case 1: return "JJR";
			case 2: return "JJS";
			case 3: return "NN";
			case 4: return "NNS";
			case 5: return "NNP";
			case 6: return "NNPS";
			case 7: return "RB";
			case 8: return "RBR";
			case 9: return "RBS";
			case 10: return "VB";
			case 11: return "VBD";
			case 12: return "VBG";
			case 13: return "VBN";
			case 14: return "VBP";
			case 15: return "VBZ";
			default: return null;
		}
	}
	
	public void setPos (String pos) {
		int index = getIndex(pos);
		if (index > -1 && index < this.vector.length) {
			this.vector[index]++;
		}
		if (posOccurence.length() > 0)
			posOccurence.append(" ");
		posOccurence.append(pos);
	}
	
	public boolean containsPos(int index) {
		return (this.vector[index] > 0);
	}
	
	public long posFrequency(int index) {
		return this.vector[index];
	}
	
	public String getOccurences() {
		return this.posOccurence.toString();
	}
	
	
	public boolean isIdentical(Object obj) {
		if (obj == null) {
	        return false;
	    }
	    if (getClass() != obj.getClass()) {
	        return false;
	    }
	    
	    return this.posOccurence.equals(((PosVector)obj).getOccurences());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
	        return false;
	    }
	    if (getClass() != obj.getClass()) {
	        return false;
	    }
	    
	    for (int i=0; i<this.vector.length; i++) {
	    	if (this.vector[i] != ((PosVector)obj).posFrequency(i))
	    		return false;
	    }
	    
	    return true;
	    
	}
	
	@Override
	public String toString() { 
		return posOccurence.toString();
	}
	
	public String toStringUnique() {
		StringBuilder me = new StringBuilder();
		for (int i=0; i<this.vector.length; i++) {
			if (this.vector[i] > 0) {
				if (me.length() > 0)
					me.append("-");
				// frequency (vector[i]) can be removed to get a coarse grained representation
				me.append(getPos(i)+"#"+this.vector[i]);	
			}
		}
		return me.toString();
	}

	@Override
	public int compareTo(Object o) {
		if (this.equals(o))
			return 0;
		return -1;
	}
	
	@Override 
	public int hashCode() {
		return this.toStringUnique().hashCode();
	}
	
}
