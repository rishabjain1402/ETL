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
- **Compare** findings to actual CDC-reported statistics

---

## Technologies Used

- Python 
- Apache Spark (PySpark, Spark SQL)  
- Pandas, NumPy  
- Matplotlib, Seaborn (for EDA visualizations)

---

## Key Features

- Schema mapping across distinct survey formats using official codebooks  
- Transformation of race, age, and gender variables for exact join alignment  
- Estimation of disease prevalence in merged dataset  
- Visual analysis of prevalence patterns across key demographic segments  
- Notebook format ready for interactive exploration

---

## Files

- `p1.py`: Spark ETL pipeline implementation  
- `brfss_input.json`: Raw BRFSS data  
- `nhis_input.csv`: Raw NHIS data  
- `test_brfss.json`, `test_nhis.csv`: Sample test datasets  
- `joined_output.csv`: Final joined dataset with mapped values  
- `eda.ipynb`: Exploratory data analysis notebook with plots and interpretation  
- `README.md`: This file

---

## Sample Visualizations

- Bar plots of disease prevalence by:
  - Race/ethnicity  
  - Gender  
  - Age group  
- Heatmap of correlations (if numeric features available)
- Comparison
