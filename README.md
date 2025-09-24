# Document Similarity Analysis with Hadoop MapReduce

## Overview
Compute Jaccard similarity between text documents from a collection using Hadoop MapReduce framework. The project provides insights into document relationships, text overlap, and content similarity patterns across multiple documents.

## Dataset Description
Three input files are used with varying sizes:
- **dataset1_1000words.txt**: Contains approximately 1000 words across multiple documents (document_id, document_text).
- **dataset2_3000words.txt**: Contains approximately 3000 words across multiple documents (document_id, document_text).
- **dataset3_5000words.txt**: Contains approximately 5000 words across multiple documents (document_id, document_text).

## Repository Structure
```
├── src/
│   └── main/
│       └── java/
│           └── DocumentSimilarity.java    # Main MapReduce implementation
├── pom.xml                               # Maven configuration
├── docker-compose.yml                    # Hadoop cluster setup
├── README.md                            # Project documentation
├── datasets/
│   ├── dataset1_1000words.txt          # Small dataset
│   ├── dataset2_3000words.txt          # Medium dataset
│   └── dataset3_5000words.txt          # Large dataset
└── results/
    ├── 3nodes/
    │   ├── dataset1_output/
    │   ├── dataset2_output/
    │   └── dataset3_output/
    └── 1node/
        ├── dataset1_output/
        ├── dataset2_output/
        └── dataset3_output/
```

## Output Directory Structure
```
results/
├── 3nodes/
│   ├── dataset1_output/
│   ├── dataset2_output/
│   └── dataset3_output/
└── 1node/
    ├── dataset1_output/
    ├── dataset2_output/
    └── dataset3_output/
```

## Tasks and Outputs
1. **Document Preprocessing**: Converts text to lowercase and removes punctuation for consistent processing. Output: Clean word sets per document.
2. **Pair Generation**: Creates all possible document pairs for similarity comparison. Output: Document pair combinations.
3. **Jaccard Calculation**: Computes similarity scores using intersection over union formula. Output: `results/[nodes]/dataset[X]_output/`
4. **Performance Analysis**: Compares execution times between 3-node and 1-node cluster configurations. Output: Timing comparisons in README.

## Execution Instructions
1. Install Java and Maven:
   ```bash
   mvn clean compile package
   ```
2. Start Hadoop cluster with 3 data nodes:
   ```bash
   docker-compose up -d
   ```
3. Run the analysis for each dataset:
   ```bash
   docker exec -it namenode hadoop jar target/document-similarity-1.0.jar DocumentSimilarity /input/dataset1_1000words.txt /output/dataset1_3nodes
   ```
4. Find results in the `results/` folder after copying from HDFS.

## Analysis Workflow
1. **Load Data**: Read document files into HDFS and process line by line.
2. **Prepare Data**: Extract document IDs and clean text content in Mapper phase.
3. **Run Analysis**:
   - Generate unique word sets for each document
   - Create all possible document pair combinations
   - Calculate Jaccard similarity for each pair
   - Format output with similarity scores
4. **Save Results**: Each dataset's output is saved in respective directories under `results/`.

## Errors and Resolutions
**Memory Management Issues:**
Large datasets caused OutOfMemory errors during processing. This was resolved by optimizing data structures and using HashSet for efficient word storage. Additionally, proper HDFS directory cleanup between runs prevented conflicts.

**Docker Container Communication:**
Initial setup had inter-container networking issues. Verified docker-compose.yml configuration and ensured proper container linking resolved connectivity problems between namenode and datanodes.

## Performance Comparison

| Dataset | 3 Data Nodes | 1 Data Node | Performance Difference |
|---------|-------------|-------------|----------------------|
| Dataset 1 (1K words) | [X] seconds | [Y] seconds | [Y-X] seconds slower |
| Dataset 2 (3K words) | [X] seconds | [Y] seconds | [Y-X] seconds slower |
| Dataset 3 (5K words) | [X] seconds | [Y] seconds | [Y-X] seconds slower |

## Notes
- The analysis processes at least 1000, 3000, and 5000 words across different dataset sizes.
- All code, datasets, and outputs are included in this repository.
- The project structure and output formatting follow assignment guidelines.
- Codespaces environment maintained active as per requirements.

## Author
Purva Jagtap
ITCS 6190/8190, Fall 2025
