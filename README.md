# Word Sense Disambiguation at Scale for Wikipedia
This suite of classes can be used to perform unsupervised WSD on plaintext versions of Wikipedia and extract dependency parse trees from the encountered sentences.
The underlying framework used is the MapReduce compatible [Ajira](https://github.com/jrbn/ajira)

## Dependencies
The project requires JDK 1.8 and is built completely with Maven (althogh some of the dependencies are local). 

The underlying WSD algorithm (Personalised Page Rank) requires Wordnet, we include Wordnet 3.0 in the data.dict package and read it from the jar (this is particularly useful when running in Hadoop).

To read Wornet, we use a small part of the [C-Cat package](https://github.com/fozziethebeat/C-Cat) which is included directly in the repository, in the gov.llnl.ontology package.  

## Getting started

Functionality is split by class, in the **SyntacticWikipedia** package:
- **ArticleDisambiguator** : creates semantically and syntactically annotated sentences from Wikipedia articles
- **SubtreeExtractor** : creates all distinct quadarcs for all sentences in Wikipedia
- **SubtreeAnalyser** : analysis on output from SubtreeExtractor
- **WordDependencyExtractor** : generates the most frequent quadarc for each word-sense pair, based on the output from output from SubtreeExtractor

As parameters, only input and output folders are required. Ajira can be run in either single-node mode or distributed using Ibis Portability Layer.
More details on this can be found in the [Ajira documentation](http://jrbn.github.io/ajira/)

**Note:** ArticleDisambiguator was used to produce the wsd-wikipedia-articles data set with over 72 million annotated sentences. A readme for the data set can be found [here](https://docs.google.com/document/d/1TxzxMHt56unEX2J9BOnPk6Hhjrc2kxAuSD2dGT-NeNA)
