package nl.vu.few.SyntacticWikipedia;

import gov.llnl.ontology.wordnet.Synset.PartsOfSpeech;

/*
 * The full Peen-Treebank tagset.
 * For reference see: https://www.ling.upenn.edu/courses/Fall_2003/ling001/penn_treebank_pos.html
 */
public enum PennTreebankTag {
	CC("CC"),		// NOT SUPPORTED: Coordinating conjunction
	CD("CD"),		// NOT SUPPORTED: Cardinal number
	DT("DT"),		// NOT SUPPORTED: Determiner
	EX("EX"),		// NOT SUPPORTED: Existential there
	FW("FW"),		// NOT SUPPORTED: Foreign word
	IN("IN"),		// NOT SUPPORTED: Preposition or subordinating conjunction
	JJ("JJ"),		// SUPPORTED: Adjective
	JJR("JJR"),		// SUPPORTED: Adjective, comparative
	JJS("JJS"),		// SUPPORTED: Adjective, superlative
	LS("LS"),		// NOT SUPPORTED: List item marker
	MD("MD"),		// NOT SUPPORTED: Modal
	NN("NN"),		// SUPPORTED: Noun, singular or mass
	NNS("NNS"),		// SUPPORTED: Noun, plural
	NNP("NNP"),		// SUPPORTED: Proper noun, singular
	NNPS("NNPS"),	// SUPPORTED: Proper noun, plural
	PDT("PDT"),		// NOT SUPPORTED: Predeterminer
	POS("POS"),		// NOT SUPPORTED: Possessive ending
	PRP("PRP"),		// NOT SUPPORTED: Personal pronoun
	PRP$("PRP$"),	// NOT SUPPORTED: Possessive pronoun
	RB("RB"),		// SUPPORTED: Adverb
	RBR("RBR"),		// SUPPORTED: Adverb, comparative
	RBS("RBS"),		// SUPPORTED: Adverb, superlative
	RP("RP"),		// NOT SUPPORTED: Particle
	SYM("SYM"),		// NOT SUPPORTED: Symbol
	TO("TO"),		// NOT SUPPORTED: to
	UH("UH"),		// NOT SUPPORTED: Interjection
	VB("VB"),		// SUPPORTED: Verb, base form
	VBD("VBD"),		// SUPPORTED: Verb, past tense
	VBG("VBG"),		// SUPPORTED: Verb, gerund or present participle
	VBN("VBN"),		// SUPPORTED: Verb, past participle
	VBP("VBP"),		// SUPPORTED: Verb, non-3rd person singular present
	VBZ("VBZ"),		// SUPPORTED: Verb, 3rd person singular present
	WDT("WDT"),		// NOT SUPPORTED: Wh-determiner
	WP("WP"),		// NOT SUPPORTED: Wh-pronoun
	WP$("WP$"),		// NOT SUPPORTED: Possessive wh-pronoun
	WRB("WRB");		// NOT SUPPORTED: Wh-adverb
	
	private String name;

	PennTreebankTag(String name) {
		this.name = name;
	}
	
	public static PennTreebankTag fromString(String name) {
	    if (name != null) {
	      for (PennTreebankTag tag : PennTreebankTag.values()) {
	        if (name.equalsIgnoreCase(tag.name)) {
	          return tag;
	        }
	      }
	    }
	    return null;
	 }
	
	 public static String tagToDescription(PennTreebankTag tag){
		switch(tag){
    		case CC: return "Coordinating conjunction";
    		case CD: return "Cardinal number";
    		case DT: return "Determiner";
    		case EX: return "Existential 'there'";
    		case FW: return "Foreign word";
    		case IN: return "Preposition or subordinating conjunction";
    		case JJ: return "Adjective";
    		case JJR: return "Adjective, comparative";
    		case JJS: return "Adjective, superlative";
    		case LS: return "List item marker";
    		case MD: return "Model";
    		case NN: return "Noun, singular or mass";
    		case NNS: return "Noun, plural";
    		case NNP: return "Proper noun, singular";
    		case NNPS: return "Proper noun, plural";
    		case PDT: return "Predeterminer";
    		case POS: return "Possesive ending";
    		case PRP: return "Personal pronoun";
    		case PRP$: return "Possesive pronoun";
    		case RB: return "Adverb";
    		case RBR: return "Adverb, comparative";
    		case RBS: return "Adverb, superlative";
    		case RP: return "Particle";
    		case SYM: return "Symbol";
    		case TO: return "'to'";
    		case UH: return "Interjection";
    		case VB: return "Verb, base form";
    		case VBD: return "Verb, past tense";
    		case VBG: return "Verb, gerund or present participle";
    		case VBN: return "Verb, past participle";
    		case VBP: return "Verb, non-3rd person singular present";
    		case VBZ: return "Verb, 3rd person singular present";
    		case WDT: return "Wh-determiner";
    		case WP: return "Wh-pronoun";
    		case WP$: return "Possesive wh-pronoun";
    		case WRB: return "Wh-adverb";
    		default: return "unknown";
		}
	}
	
	 /*
     * Converts from Penn Treebank part of speech tags to Wordnet part of speech. 
     * From Penn Treebank 16 out of 36 tags have Wordnet equivalents
     */
	public static PartsOfSpeech TagToWordnet(String tag){
		if(tag.startsWith("NN")){
			return PartsOfSpeech.NOUN;
		}
		else if(tag.startsWith("JJ")){
			return PartsOfSpeech.ADJECTIVE;
		}
		else if(tag.startsWith("V")){
			return PartsOfSpeech.VERB;
		}
		else if(tag.startsWith("RB")){
			return PartsOfSpeech.ADVERB;
		}
		return null;		
	}
	
	public static boolean isTagConversionSupported(String tag){
		if(tag.startsWith("NN")){
			return true;
		}
		else if(tag.startsWith("JJ")){
			return true;
		}
		else if(tag.startsWith("V")){
			return true;
		}
		else if(tag.startsWith("RB")){
			return true;
		}
		return false;		
	}
}
