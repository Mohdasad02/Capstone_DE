# Databricks notebook source
from pyspark.sql.functions import (
    col, concat, lit, length,
    substring, regexp_replace, locate
)

def mask_phone(df, column="phone"):
    """
    masks first 6 digits of phone number
    9876543210 → ******3210
    """
    return df.withColumn(
        column,
        concat(
            lit("******"),
            substring(col(column).cast("string"), 7, 4)
        )
    )

def mask_email(df, column="email"):
    """
    keeps first letter and domain, masks everything in between
    ankit.gupta123@gmail.com → a*************@gmail.com
    """
    from pyspark.sql.functions import expr
    return df.withColumn(
        column,
        expr(f"""
            concat(
                substring({column}, 1, 1),
                repeat('*', locate('@', {column}) - 2),
                substring({column}, locate('@', {column}), length({column}))
            )
        """)
    )

def mask_all_pii(df):
    """
    master function - applies all masking rules
    name  → kept as is
    phone → first 6 digits masked
    email → masked upto @ keeping first letter and domain
    """
    df = mask_phone(df)
    df = mask_email(df)
    return df

print("PII masking functions loaded")
print("Rules:")
print("  name  → unchanged")
print("  phone → ******XXXX (last 4 visible)")
print("  email → X*****@domain.com (first letter + domain visible)")