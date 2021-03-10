
## PubMed Author Name Disambiguation
This project is developed for PubMed author name disambiguation. The name ambiguity problem can be understood that tow authors with same (similar) name in different citation are often ambiguous to tell from.  Name ambiguity problem is serious in PubMed, because there are nearly 10,000 namespaces (last name and firs initial, also known as blocks) with size over 1,000 in PubMed as of 2019.  This problem not only hinders the communication of valuable discoveries produced by others in biomedical field, but also restricts many downstream researches or applications, such as author-centric bibliometric analysis and expert identification.

### Setup
The project is mainly implemented by Python 3.6, we used following packages.
  
- scipy, numpy, pandas, sklearn, 
- clickhouse-driver==0.2.0
- geograpy3==0.1.24
- jaro-winkler==2.0.0
- python-Levenshtein==0.12.0
- nltk==3.5

The Python module can extracted most features in use, and develop disambiguation model  using machine learning models, while for some features, extracting them from raw input has already implemented by other language, such as Java. Thus, to integrate these features, "Dependency-Feature" is a Java-based module, which can extract these dependent features. Note that "maui" in this folder, is a keyword-generation tool.  "tc2011" can extract Journal Descriptors and Semantic Types for each PubMed citation. Besides, this module also detect geographic fields from author affiliation using NER technique, provided by "stanford-corenlp" (see dependencies in pom.xml).

### Database
The "database" folder contains a bunch of sql scripts, their names are self-explainable. These scripts aim to associate additional metadata from external databases for the gold standard datasets, thus, some steps including "database linkages", "metadata extraction", "author profile building" are implemented here.  

### Collected Resources
The "resources" folder contains necessary resources during developing this project. 
The two validation datasets did not contain any other metadata apart from the author names, positions. To obtain more discriminative information, we developed a program 
to crawl from PubMed official site.  The XML format citations for the datasets are included in "gs-dataset-articles" and "song-dataset-articles".
