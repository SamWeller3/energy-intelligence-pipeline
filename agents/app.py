import os
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from databricks import sql
import anthropic

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_SQL_TOKEN = os.getenv("DATABRICKS_SQL_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

st.set_page_config(page_title="Energy Intelligence Assistant", layout="wide")
st.title("⚡ Energy Intelligence Assistant")
st.write("Ask questions about electricity prices, weather, and generation across all 50 states.")

SCHEMA_DESCRIPTION = """
Table: gold_monthly_state_summary

Columns:
- state (string): 2-letter US state code, e.g. 'TX', 'CA'
- year (integer): e.g. 2023
- month (integer): 1-12
- price (decimal): average electricity price, all sectors combined, cents per kWh
- price_units (string): unit description for price
- price_residential (decimal): residential electricity price, cents per kWh
- price_commercial (decimal): commercial electricity price, cents per kWh
- price_industrial (decimal): industrial electricity price, cents per kWh
- price_transportation (decimal): transportation electricity price, cents per kWh
- price_other (decimal): other sector electricity price, cents per kWh
- avg_temp_max_f (decimal): average daily high temperature, Fahrenheit
- avg_temp_min_f (decimal): average daily low temperature, Fahrenheit
- avg_temp_f (decimal): average daily temperature, Fahrenheit (NOAA-reported value if available, otherwise derived from max/min)
- highest_temp_f (decimal): hottest single day that month, Fahrenheit
- lowest_temp_f (decimal): coldest single day that month, Fahrenheit
- total_precipitation_inches (decimal): total precipitation for the month, inches
- total_snowfall_inches (decimal): total snowfall for the month, inches
- avg_wind_speed_mph (decimal): average wind speed, mph
- days_with_precipitation (integer): count of days with measurable precipitation
- days_with_snow (integer): count of days with measurable snowfall
- generation_natural_gas (decimal): electricity generated from natural gas, thousand megawatthours
- generation_coal (decimal): electricity generated from coal, thousand megawatthours
- generation_nuclear (decimal): electricity generated from nuclear, thousand megawatthours
- generation_solar (decimal): electricity generated from solar, thousand megawatthours
- generation_wind (decimal): electricity generated from wind, thousand megawatthours
- generation_hydro (decimal): electricity generated from hydroelectric, thousand megawatthours

This table has one row per state per month from January 2020 through mid-2026.
"""


def sanitize_sql(sql_query):
    """Replace any stray Unicode operators Claude might produce with valid ASCII SQL operators."""
    replacements = {
        "≥": ">=",
        "≤": "<=",
        "≠": "!=",
    }
    for bad, good in replacements.items():
        sql_query = sql_query.replace(bad, good)
    return sql_query


def generate_sql(question):
    prompt = f"""You are a SQL expert. Given the table schema below, write a SQL query to answer the user's question.

{SCHEMA_DESCRIPTION}

Rules:
- Only output the SQL query, nothing else. No explanation, no markdown formatting, no backticks.
- Use standard SQL compatible with Databricks SQL.
- Use only standard ASCII comparison operators: >=, <=, !=, =, <, > — never Unicode symbols like ≥, ≤, or ≠.
- Always limit results to a reasonable number of rows (e.g. LIMIT 100) unless the question requires aggregation.
- If the question asks about correlation, relationship, or whether two things are related, use the CORR() aggregate function to compute an actual correlation coefficient between the two relevant numeric columns, rather than just selecting raw rows. Return the correlation as its own named column (e.g. CORR(avg_temp_f, price) AS correlation_coefficient), grouped appropriately if the question implies a breakdown (e.g. by state), or as a single value if it's an overall question.
- Also include the underlying raw columns used in the correlation (e.g. avg_temp_f, price) so the data can still be charted as a scatter plot.

Question: {question}

SQL Query:"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return sanitize_sql(response.content[0].text.strip())


def decide_chart_type(question, df):
    """Ask Claude whether/how to visualize the results."""
    prompt = f"""Given this user question and the resulting data columns, decide the best way to visualize it.

Question: {question}
Columns: {list(df.columns)}
Number of rows: {len(df)}
Sample data:
{df.head(10).to_string()}

Respond with ONLY one of these options, nothing else:
- "none" (if a table is better, e.g. single row or single value)
- "line:<x_column>:<y_column>" (for trends over time, e.g. line:month:price)
- "bar:<x_column>:<y_column>" (for comparisons across categories, where each x value has exactly ONE row)
- "grouped_bar:<x_column>:<y_column>:<group_column>" (when there are multiple categories repeated across another dimension, e.g. comparing groups across several years. x_column should be the dimension you want side by side clusters for, e.g. the year. group_column should be what splits each cluster into separate colored bars, e.g. the category being compared. Example: grouped_bar:year:avg_price_cents_per_kwh:generation_profile means each year gets its own cluster on the x-axis, with one bar per generation_profile inside that cluster.)
- "scatter:<x_column>:<y_column>" (for relationships/correlations between two numeric variables)

Look carefully at the sample data: if the same value in one column appears alongside multiple different values in another column, that second column is likely the group_column, and the first is likely the x_column.

Your answer:"""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=80,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()


def render_chart(chart_instruction, df):
    if chart_instruction == "none" or ":" not in chart_instruction:
        return False

    parts = chart_instruction.split(":")
    chart_type = parts[0]

    if chart_type == "grouped_bar" and len(parts) == 4:
        _, x_col, y_col, group_col = parts
        if x_col not in df.columns or y_col not in df.columns or group_col not in df.columns:
            return False
        df = df.copy()
        df[x_col] = df[x_col].astype(str)  # force categorical x-axis, not numeric
        fig = px.bar(df, x=x_col, y=y_col, color=group_col, barmode="group")
        st.plotly_chart(fig, use_container_width=True)
        return True

    if len(parts) != 3:
        return False

    chart_type, x_col, y_col = parts

    if x_col not in df.columns or y_col not in df.columns:
        return False

    if chart_type == "line":
        fig = px.line(df, x=x_col, y=y_col, markers=True)
    elif chart_type == "bar":
        fig = px.bar(df, x=x_col, y=y_col)
    elif chart_type == "scatter":
        fig = px.scatter(df, x=x_col, y=y_col)
    else:
        return False

    st.plotly_chart(fig, use_container_width=True)
    return True


def interpret_results(question, df):
    """Ask Claude to explain the results in plain English, with statistical sanity checks."""
    sample = df.head(20).to_string()
    prompt = f"""A user asked: "{question}"

Here is a sample of the SQL query results (showing up to 20 rows of {len(df)} total):

{sample}

Write a short, direct, plain-English answer to their question based on this data.

Rules:
- If the results compare groups (e.g. different categories, states, or profiles), state the comparison clearly and separately from any correlation/trend finding — do not blend "group A vs group B" with "X correlates with Y" in the same sentence, since these are different claims.
- If you see a column like num_states, state_count, or similar that indicates sample size, and the sizes are meaningfully different between groups being compared (e.g. one group has 3x or more rows/states than another), explicitly flag this as a caveat — small or unbalanced sample sizes make the comparison less reliable.
- If the results include a correlation coefficient column, state its value and describe the strength and direction (e.g. weak positive, strong negative).
- Keep it to 3-5 sentences. Do not repeat the question back. Do not add generic disclaimers about needing more data — only mention sample size if it's actually imbalanced in this specific result."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()


def run_query(sql_query):
    connection = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_SQL_TOKEN,
    )
    cursor = connection.cursor()
    cursor.execute(sql_query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return columns, rows


question = st.text_input("Ask a question:", placeholder="What was the average electricity price in Texas in 2023?")

if st.button("Ask"):
    if question:
        with st.spinner("Generating SQL..."):
            generated_sql = generate_sql(question)

        st.code(generated_sql, language="sql")

        try:
            with st.spinner("Running query..."):
                columns, rows = run_query(generated_sql)

            df = pd.DataFrame(rows, columns=columns)

            if len(df) > 0:
                with st.spinner("Interpreting results..."):
                    answer = interpret_results(question, df)
                st.markdown(f"### Answer\n{answer}")

            st.write("Data:")
            st.dataframe(df)

            if len(df) > 1:
                with st.spinner("Deciding how to visualize..."):
                    chart_instruction = decide_chart_type(question, df)
                render_chart(chart_instruction, df)

        except Exception as e:
            st.error(f"Query failed: {e}")
    else:
        st.warning("Please enter a question.")