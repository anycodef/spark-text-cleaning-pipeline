# Text Cleaning Pipeline (Caso6)

This project implements a **text cleaning pipeline** using **PySpark**, designed to process and standardize text data such as social media posts.  

The pipeline performs the following cleaning steps:

1. **Remove URLs and Mentions**  
   - Removes web links and `@mentions` from the text.  

2. **Emoji Normalization**  
   - Collapses repeated emojis into a single occurrence.  
   - Replaces emojis with descriptive words (using `demoji`).  
   - Translates emoji descriptions into Spanish (`deep-translator`).  
   - Ensures proper spacing when emojis are attached to words.  

3. **Hashtag Standardization**  
   - Keeps the `#` symbol.  
   - Translates hashtags into Spanish.  
   - Converts spaces into underscores and applies lowercase.  

4. **Letter Repetition Reduction**  
   - Reduces sequences of 3+ repeated letters to a single one.  
   - Example: `"hooooola"` → `"hola"`.  

5. **Export**  
   - Supports exporting the cleaned dataset into:
     - **CSV**  
     - **Excel (.xlsx)**  
     - **Parquet**  

---

## ⚙️ Requirements

Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

---

## 🚀 Usage

Run the pipeline as a command-line script:

```bash
python3 pipeline_cleaning.py --csv input.csv --export-to-csv
```

### Examples

- **Export to CSV with automatic naming**  
  ```bash
  python3 pipeline_cleaning.py --csv Datos_Caso6.csv --export-to-csv
  ```
  → generates `Datos_Caso6-clean.csv`

- **Export to Excel with custom name**  
  ```bash
  python3 pipeline_cleaning.py --csv Datos_Caso6.csv --export-to-excel clean_output.xlsx
  ```

- **Export to multiple formats at once**  
  ```bash
  python3 pipeline_cleaning.py --csv Datos_Caso6.csv --export-to-csv --export-to-parquet
  ```

---

## 📂 Project Structure

```
.
├── pipeline_cleaning.py    # Main pipeline script
├── requirements.txt        # Dependencies
├── README.md               # Documentation
└── input.csv               # Example input file
```

---

## 📝 Notes

- Input must be a **CSV file** with at least one column named `text`.  
- Output naming: if no name is provided, the pipeline appends `-clean` to the input file name.  
- Example: `Datos_Caso6.csv` → `Datos_Caso6-clean.csv`.  

---
