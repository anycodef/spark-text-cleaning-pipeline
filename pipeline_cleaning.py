#!/usr/bin/env python3
import argparse
import os
import re
import regex
import demoji
from deep_translator import GoogleTranslator
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# ---------------------------
# Configuración inicial
# ---------------------------
demoji.download_codes()
translator = GoogleTranslator(source="en", target="es")

# ---------------------------
# Funciones de limpieza
# ---------------------------

def remove_urls_mentions(df):
    df = df.withColumn(
        "text_clean",
        F.regexp_replace("text", r"http\S+|www\.\S+", "")
    )
    df = df.withColumn(
        "text_clean",
        F.regexp_replace("text_clean", r"@\w+", "")
    )
    return df

def replace_emojis_translate(text):
    if text is None:
        return None
    text = regex.sub(r'(\X)\1+', r'\1', text)  # Colapsar repetidos
    replacements = demoji.findall(text)
    for emo, desc in replacements.items():
        try:
            desc_es = translator.translate(desc)
        except Exception:
            desc_es = desc
        desc_token = desc_es.lower()
        text = regex.sub(fr'(?<=\w){regex.escape(emo)}', f' {desc_token}', text)
        text = regex.sub(fr'{regex.escape(emo)}(?=\w)', f'{desc_token} ', text)
        text = text.replace(emo, desc_token + " ")
    return regex.sub(r'\s+', ' ', text).strip()

replace_emojis_udf = F.udf(replace_emojis_translate, StringType())

def normalize_emojis(df):
    return df.withColumn("text_clean", replace_emojis_udf("text_clean"))

def normalize_hashtags_translate(text):
    if text is None:
        return None
    hashtags = re.findall(r"#\w+", text)
    for h in hashtags:
        palabra = h.lstrip("#")
        try:
            traducido = translator.translate(palabra)
            nuevo_h = "#" + traducido.lower().replace(" ", "_")
            text = text.replace(h, nuevo_h)
        except Exception:
            text = text.replace(h, h.lower())
    return text

normalize_hashtags_udf = F.udf(normalize_hashtags_translate, StringType())

def normalize_hashtags(df):
    return df.withColumn("text_clean", normalize_hashtags_udf("text_clean"))

def reduce_letter_repetitions(df):
    return df.withColumn(
        "text_clean",
        F.regexp_replace("text_clean", r"(\p{L})\1{2,}", r"$1")
    )

# ---------------------------
# Pipeline principal
# ---------------------------
def clean_pipeline(df):
    df = remove_urls_mentions(df)
    df = normalize_emojis(df)
    df = normalize_hashtags(df)
    df = reduce_letter_repetitions(df)
    return df

# ---------------------------
# Exportación
# ---------------------------
def export_results(df_spark, output_path, fmt):
    if fmt == "csv":
        df_spark.toPandas().to_csv(output_path, index=False, encoding="utf-8")
    elif fmt == "excel":
        df_spark.toPandas().to_excel(output_path, index=False, engine="openpyxl")
    elif fmt == "parquet":
        df_spark.write.mode("overwrite").parquet(output_path)
    else:
        raise ValueError("Formato no soportado: " + fmt)

# ---------------------------
# Main CLI
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Text cleaning pipeline (Caso6)")
    parser.add_argument("input_file", type=str, help="Input file (.csv or .xlsx)")
    parser.add_argument("--export-to-csv", nargs="?", const=True, help="Export to CSV (optional: filename)")
    parser.add_argument("--export-to-excel", nargs="?", const=True, help="Export to Excel (optional: filename)")
    parser.add_argument("--export-to-parquet", nargs="?", const=True, help="Export to Parquet (optional: filename)")
    args = parser.parse_args()

    # Crear sesión Spark
    spark = SparkSession.builder.appName("Caso6-LimpiezaTexto").getOrCreate()

    # Detectar formato de entrada
    ext = os.path.splitext(args.input_file)[1].lower()
    if ext == ".csv":
        df = pd.read_csv(args.input_file)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(args.input_file, engine="openpyxl")
    else:
        raise ValueError("Formato de entrada no soportado. Usa .csv o .xlsx")

    df_spark = spark.createDataFrame(df)

    # Aplicar pipeline
    df_clean = clean_pipeline(df_spark)

    # Nombre base de salida
    input_base, _ = os.path.splitext(args.input_file)

    if args.export_to_csv is not None:
        output_file = args.export_to_csv if isinstance(args.export_to_csv, str) else f"{input_base}-clean.csv"
        export_results(df_clean, output_file, "csv")

    if args.export_to_excel is not None:
        output_file = args.export_to_excel if isinstance(args.export_to_excel, str) else f"{input_base}-clean.xlsx"
        export_results(df_clean, output_file, "excel")

    if args.export_to_parquet is not None:
        output_file = args.export_to_parquet if isinstance(args.export_to_parquet, str) else f"{input_base}-clean.parquet"
        export_results(df_clean, output_file, "parquet")

    print("✅ Cleaning completed and file exported.")

if __name__ == "__main__":
    main()

