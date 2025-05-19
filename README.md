# Health Survey Analysis: ETL and Disease Prevalence Estimation

This project performs an end-to-end ETL and exploratory data analysis (EDA) pipeline on two national public health datasets from the U.S. Centers for Disease Control and Prevention (CDC):  
- **BRFSS (Behavioral Risk Factor Surveillance System)**  
- **NHIS (National Health Interview Survey)**

The goal is to estimate the prevalence of a chronic disease across demographic subgroups by combining complementary features from both datasets and analyzing the resulting population.

---

## Project Overview

- **Extract** raw `.json` and `.csv` files into Spark DataFrames  
- **Transform** features based on CDC codebooks to align value formats across datasets  
- **Join** datasets on shared demographics (e.g., age, race, gender)  
- **Load** into a unified DataFrame with new disease prevalence information  
- **Analyze** disease prevalence by race, gender, and age group  
- **Model** diabetes status using classification algorithms  
- **Compare** findings to actual CDC-reported statistics  

---

## Technologies Used

- Python  
- Apache Spark (PySpark, Spark SQL)  
- Pandas, NumPy  
- Matplotlib, Seaborn, Plotly (for EDA visualizations)  
- Spark MLlib (Logistic Regression, Random Forest, Decision Tree)  

---

## Key Features

- Schema mapping across distinct survey formats using official codebooks  
- Transformation of race, age, and gender variables for exact join alignment  
- Estimation of disease prevalence in merged dataset  
- Visual analysis of prevalence patterns across key demographic segments  
- Simple machine learning models for diabetes classification based on demographics  
- Notebook format ready for interactive exploration and replication  

---

## Files

- `p1.py`: Spark ETL pipeline implementation  
- `brfss_input.json`: Raw BRFSS data  
- `nhis_input.csv`: Raw NHIS data  
- `test_brfss.json`, `test_nhis.csv`: Sample test datasets  
- `output/`: Generated outputs (includes summaries and full join)  
  - `prevalence_by_sex/`, `prevalence_by_age/`, `prevalence_by_race/` – tracked  
  - `joined_data/` – **ignored** in Git due to size (~8 GB)  
- `eda.ipynb`: Exploratory data analysis notebook with plots, interpretation, and machine learning classifications
- `README.md`: This file  

---

## Sample Visualizations

- Bar plots of disease prevalence by:
  - Race/ethnicity  
  - Gender  
  - Age group  
- Heatmap of prevalence across age and race  
- Sankey diagram showing flows from age → race → diabetes status  

---

## How to Run

1. Place the raw data files in a `data/` folder
2. Run the ETL pipeline with Spark:
```bash
python3 p1.py data/nhis_input.csv data/brfss_input.json
3. Open eda.ipynb to explore the data visually
