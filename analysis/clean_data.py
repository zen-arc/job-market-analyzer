# clean_data.py
# This script takes our raw scraped data and cleans it
# Think of this as the "washing and sorting" step before cooking

import pandas as pd
import json
import os

# ============================================================
# STEP 1: LOAD THE RAW DATA
# ============================================================

print("Loading raw data...")

# Load our scraped JSON file into a Pandas DataFrame
# Like opening an Excel file in Python
df = pd.read_json("data/raw/jobs_raw.json")

print(f"Raw data loaded: {len(df)} jobs")
print(f"Columns: {list(df.columns)}")
print()

# ============================================================
# STEP 2: FIRST LOOK AT THE DATA
# ============================================================

print("=== FIRST LOOK ===")
print(df.head(3))  # Show first 3 rows
print()

# Check how many empty values each column has
print("=== MISSING VALUES ===")
print(df.isnull().sum())
print()

# ============================================================
# STEP 3: REMOVE DUPLICATES
# ============================================================

print("Removing duplicates...")

before = len(df)

# Remove rows where job_url is identical
# Same URL = same job posted twice
df = df.drop_duplicates(subset=["job_url"])

after = len(df)
print(f"Removed {before - after} duplicate jobs")
print(f"Remaining: {after} jobs")
print()

# ============================================================
# STEP 4: CLEAN JOB TITLES
# ============================================================

print("Cleaning job titles...")

# Remove extra spaces and standardize capitalization
df["job_title"] = df["job_title"].str.strip()
df["job_title"] = df["job_title"].str.title()

# ============================================================
# STEP 5: CLEAN COMPANY NAMES
# ============================================================

print("Cleaning company names...")

df["company_name"] = df["company_name"].str.strip()
df["company_name"] = df["company_name"].str.title()

# ============================================================
# STEP 6: CLEAN AND STANDARDIZE LOCATIONS
# ============================================================

print("Standardizing locations...")

# First clean whitespace
df["location"] = df["location"].str.strip()

# Many jobs have multiple locations like "Pune, Bengaluru"
# We'll keep the first location only for simplicity
df["location"] = df["location"].str.split(",").str[0].str.strip()

# Now standardize common variations
# Real life: "Bengaluru" and "Bangalore" are the same city
location_mapping = {
    "Bangalore": "Bengaluru",
    "Bengaluru/Bangalore": "Bengaluru",
    "Delhi/Ncr": "Delhi NCR",
    "Delhi/NCR": "Delhi NCR",
    "New Delhi": "Delhi NCR",
    "Hyderabad/Secunderabad": "Hyderabad",
    "Mumbai": "Mumbai",
    "Bombay": "Mumbai",
    "Chennai": "Chennai",
    "Madras": "Chennai",
    "Kolkata": "Kolkata",
    "Calcutta": "Kolkata",
}

# Replace each variation with the standard name
df["location"] = df["location"].replace(location_mapping)

# Fill empty locations
df["location"] = df["location"].fillna("Not specified")
df["location"] = df["location"].replace("", "Not specified")

# ============================================================
# STEP 7: CLEAN SALARY AND EXTRACT NUMBERS
# ============================================================

print("Processing salary data...")

# We need to extract numbers from salary_range
# "INR 360000 - 680000" → low=360000, high=680000

def extract_salary(salary_str):
    """
    Takes a salary string and returns (low, high) as numbers
    If not disclosed, returns (0, 0)
    
    Think of this like a translator:
    Input:  "INR 360000 - 680000"
    Output: (360000, 680000)
    """
    if salary_str == "Not disclosed" or pd.isna(salary_str):
        return 0, 0
    
    try:
        # Remove "INR " prefix
        cleaned = salary_str.replace("INR ", "").strip()
        # Split on " - " to get two numbers
        parts = cleaned.split(" - ")
        low = int(float(parts[0]))
        high = int(float(parts[1]))
        # If either is -1 or negative, treat as not disclosed
        if low < 0 or high < 0:
            return 0, 0
        return low, high
    except:
        return 0, 0

# Apply this function to every row
df["salary_low"] = df["salary_range"].apply(
    lambda x: extract_salary(x)[0]
)
df["salary_high"] = df["salary_range"].apply(
    lambda x: extract_salary(x)[1]
)

# Convert to lakhs (easier to read)
# 1 Lakh = 100,000 rupees
df["salary_low_lpa"] = (df["salary_low"] / 100000).round(2)
df["salary_high_lpa"] = (df["salary_high"] / 100000).round(2)

# Create a readable salary column
df["salary_clean"] = df.apply(
    lambda row: (
        f"{row['salary_low_lpa']} - {row['salary_high_lpa']} LPA"
        if row["salary_low"] > 0
        else "Not disclosed"
    ),
    axis=1
)

disclosed_count = (df["salary_low"] > 0).sum()
print(f"Jobs with salary disclosed: {disclosed_count}")
print(f"Jobs without salary: {len(df) - disclosed_count}")
print()

# ============================================================
# STEP 8: CLEAN EXPERIENCE
# ============================================================

print("Cleaning experience data...")

df["experience_required"] = df["experience_required"].str.strip()
df["experience_required"] = df["experience_required"].fillna(
    "Not specified"
)

# Extract minimum experience as a number for analysis
def extract_min_exp(exp_str):
    """
    "3 - 7 Yrs" → 3
    "Not specified" → -1
    """
    try:
        return int(exp_str.split("-")[0].strip().split(" ")[0])
    except:
        return -1

df["min_experience"] = df["experience_required"].apply(
    extract_min_exp
)

# ============================================================
# STEP 9: CLEAN SKILLS
# ============================================================

print("Cleaning skills...")

df["required_skills"] = df["required_skills"].str.strip()
df["required_skills"] = df["required_skills"].fillna("")

# Count number of skills per job
df["skills_count"] = df["required_skills"].apply(
    lambda x: len(x.split(",")) if x else 0
)

# ============================================================
# STEP 10: CLEAN DATES
# ============================================================

print("Cleaning dates...")

# Convert date strings to proper datetime objects
# Like converting "13-03-2026" text into an actual calendar date
df["date_posted"] = pd.to_datetime(
    df["date_posted"],
    errors="coerce"  # If conversion fails, put NaT (empty date)
)

# ============================================================
# STEP 11: FINAL CHECK
# ============================================================

print()
print("=== CLEANED DATA SUMMARY ===")
print(f"Total jobs after cleaning: {len(df)}")
print()
print("Sample of cleaned data:")
print(df[["job_title", "company_name", "location", "salary_clean"]].head(5))
print()
print("=== COLUMN LIST ===")
print(list(df.columns))

# ============================================================
# STEP 12: SAVE CLEANED DATA
# ============================================================

print()
print("Saving cleaned data...")

# Make sure output folder exists
os.makedirs("data/processed", exist_ok=True)

# Save as CSV — easier to work with for analysis
df.to_csv("data/processed/jobs_cleaned.csv", index=False)

print("✅ Saved to data/processed/jobs_cleaned.csv")
print()
print("=== DONE! ===")