from extract import extract_tables

pdf_path = r"C:\Users\silly\OneDrive\School\2025_Fall\MQP\MappingGlobalChinaMQP\Task_1\USCC_report.pdf"
df_raw = extract_tables(pdf_path)
print(df_raw.head())
