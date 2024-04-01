# Big Data Analysis with Apache Spark: News Article Filtering and Ranking

## Overview

This repository contains the implementation of a batch-based text search and filtering pipeline developed for a Big Data course assessed exercise. The pipeline, built using Apache Spark, ranks documents by relevance to user-defined queries and filters out similar documents to return the top 10 documents for each query. The application processes a large set of text documents, applying text preprocessing techniques such as stopword removal and stemming, and scores documents using the DPH ranking model to ensure relevancy and uniqueness in the results.

## Features

- **Document and Query Processing**: Transforms text documents and queries by removing stopwords and applying stemming to standardize terms.
- **DPH Scoring**: Utilizes the Document-Personalized-Home (DPH) scoring model to rank documents based on their relevance to each query.
- **Text Preprocessing**: Incorporates a static text pre-processor to tokenize input text, remove stopwords, and apply stemming.
- **Redundancy Removal**: Analyzes document rankings to remove near-duplicate documents based on title similarity, ensuring diversity in the top documents returned for each query.

## Structures

- `NewsArticleQueryDPHScores`: Encapsulates DPH scores for a news article query, linking relevant metadata and maintaining a map of scores.
- `NewsArticleDPHScores`: Represents DPH scores associated with a news article document.
- `NewsArticleDocument`: Represents a news article document, including a map of term counts.

## Functions

- `TextTokenizer`: Transforms items within an RDD of NewsArticle objects into corresponding NewsArticleDocument objects.
- `TermGroupsCounter`: Formats term groups by counting total occurrences of each term group.
- `SumReducer`: Custom implementation to sum values of Tuple2<String, Integer> objects.
- `NewsArticleTextDistanceFiltering`: Removes redundant documents based on text similarity.
- And others focused on implementing the core functionality of the pipeline.

## Usage

### Requirements

- Apache Spark
- Java SDK

### Running the Application

1. Clone the repository to your local machine.
2. Ensure Apache Spark and Java SDK are installed and configured.
3. Navigate to the project directory.
4. Download the dataset from https://drive.google.com/drive/folders/1ZUKFlzAPpEX9A71oCgwlXVyP3pJCa3j4?usp=sharing and add it to /data/ folder
5. Run the Spark application using the following command:

   ```
   spark-submit --class MainApplication target/your-jar-file.jar
   ```

   Replace `your-jar-file.jar` with the path to the compiled JAR file of the project.

## Dataset

The application is designed to process a corpus of news articles from the Washington Post, along with a set of user-defined queries. Two versions of the dataset are used: a local sample for development and testing, and a full dataset for evaluation.

## Development

This project was developed as part of the Big Data (H/M) Assessed Exercise for a Big Data course. The implementation was carried out following the specifications provided in the course materials, focusing on code functionality, quality, and efficiency.

## Authors

- Deepanshu Jain
- Philip Joseph

## Acknowledgments

Special thanks to the course instructors and TAs for their guidance and support throughout the project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
