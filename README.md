# COMP4804_Project
This repository contains the complete source code for COMP4804 Data Analytics Project: Information Extraction through Knowledge Graphs. 
The pipeline and usage for each stage in the project is as follows:

1. FilingScraper
This contains code for scraping filing information from the Securities and Exchange Commission website. After specifying company codes and their tickers in companies_list.txt, running `python edgar.py` would output and postprocess filing text and store them in the output_files folder.

2. KGAnnotationTool
This application was used to annotate training and test samples for model training. Running 'streamlit run app.py' would start the application within a browser.

3. Relation extraction
Running relation_extraction.py would use the  relation extraction model to generate relation triplets from the scraped text. The relation extraction model is created based on REBEL, which is contained in 'RelationExtraction'. 

4. Data postprocessing and Knowledge Graph Construction
the e2e_relation_extraction notebook cleans the extracted triplets and feeds them into the knowledge graph hosted by Neo4j.
