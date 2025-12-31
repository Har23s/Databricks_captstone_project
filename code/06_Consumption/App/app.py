import os
import io
import pandas as pd
import streamlit as st
from PIL import Image
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# --- 1. DATABRICKS CONNECTION SETUP ---
WAREHOUSE_ID = os.getenv('DATABRICKS_WAREHOUSE_ID')

@st.cache_resource
def get_sql_conn():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate
    )

def query_data(query):
    with get_sql_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            # Efficiently convert SQL results to Pandas
            return cursor.fetchall_arrow().to_pandas()

# --- 2. UI STYLING & NAVIGATION ---
st.set_page_config(layout="wide", page_title="Insurance Claims project")

with st.sidebar:
    st.title(" Insurance Claims Investigation")
    mode = st.radio("Navigation", ["Analysis Dashboard", "Submit New Claim"])
    st.divider()
    st.info(f"Connected to Warehouse: {WAREHOUSE_ID[:8]}...")

# --- 3. ANALYSIS DASHBOARD VIEW ---
if mode == "Analysis Dashboard":
    st.header("Claims Investigation Analysis")
    
    # Claim Navigation (Search)
    claim_id = st.text_input("Claim Navigation", value="53-major (2).png")
    
    # Fetch Data from Gold Table (claim_insights)
    df = query_data(f"SELECT * FROM insurance_claim_project.gold.claim_insights WHERE path LIKE '%{claim_id}%' LIMIT 1")
    
    if not df.empty:
        row = df.iloc[0]
        
        # Rule Engine Test Results (Top Metrics)
        st.write("### ⚙️ Rule Engine Test Results")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Severity Check", "MATCH" if row.get('reported_severity_check') else "MISMATCH")
        c2.metric("Policy Amount", "WITHIN LIMIT" if row.get('valid_amount') else "OVER LIMIT")
        c3.metric("Policy Date", "ACTIVE" if row.get('valid_date') else "EXPIRED")
        c4.metric("Speed Check", "PASS" if row.get('speed_check') else "FAIL")
        
        st.divider()
        
        # Details & Image Section
        left, mid, right = st.columns([1, 1.5, 1])
        with left:
            st.write("### 📝 Claim Details")
            st.write(f"**Path:** {row['path']}")
            st.write(f"**Label:** {row.get('label', 'N/A')}")
            # Risk logic from project screenshots
            risk = "LOW" if row.get('fund_release') else "HIGH"
            st.info(f"**Risk Assessment:** {risk}")

        with mid:
            st.write("### 🖼️ Accident Image")
            # Fetch binary content from Silver table
            img_df = query_data(f"SELECT content FROM insurance_claim_project.silver.training_images WHERE path = '{row['path']}'")
            if not img_df.empty:
                image = Image.open(io.BytesIO(img_df.iloc[0]['content']))
                st.image(image, use_container_width=True, caption="Verified Accident Evidence")

        with right:
            st.write("### 👤 Customer Information")
            st.write("**Name:** demo")
            st.write("**Policy:** 22222")
            st.write("**Location:** Munich")
    else:
        st.warning("No record found. Please enter a valid Claim ID.")

# --- 4. SUBMISSION MODE ---
else:
    st.header("Submit Your Claim")
    with st.container(border=True):
        st.file_uploader("Upload car damage image")
        st.text_input("Policy Number")
        st.number_input("Claim Amount ($)")
        if st.button("Submit for AI Analysis"):
            st.balloons()
            st.success("Analysis Complete: Your claim has been approved!")

# --- 5. OVERVIEW TABLE ---
st.divider()
st.write("### 📄 All Claims Management")
all_data = query_data("SELECT path, label, valid_amount, fund_release FROM insurance_claim_project.gold.claim_insights LIMIT 10")
st.dataframe(all_data, use_container_width=True)
