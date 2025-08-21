# Required Libraries
import pandas as pd
import json
import os
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine

# '''Path_Agg_insu = r"C:\Users\Priyadharshini\Documents\Usecase1\pulse\data\aggregated\insurance\country\india\state"
# Agg_insu_list = os.listdir(Path_Agg_insu)'''
path = r"state"
Agg_trans_list = os.listdir(path)

clm = {
    'State': [], 
    'Year': [], 
    'Quarter': [], 
    'Transaction_type': [], 
    'Transaction_count': [], 
    'Transaction_amount': []
}

for i in Agg_trans_list:
    p_i = os.path.join(path, i)
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = os.path.join(p_i, j)
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)
            if D['data']['transactionData'] is not None:
                for z in D['data']['transactionData']:
                    clm['Transaction_type'].append(z['name'])
                    clm['Transaction_count'].append(z['paymentInstruments'][0]['count'])
                    clm['Transaction_amount'].append(z['paymentInstruments'][0]['amount'])
                    clm['State'].append(i)
                    clm['Year'].append(int(j))
                    clm['Quarter'].append(int(k.strip('.json')))

# Create DataFrame
Agg_Trans = pd.DataFrame(clm)
#print(p_k)
#print(Agg_Trans)
# Agg_Trans.to_csv("Agg_Trans_full.csv", index=False)
# file_path = os.path.abspath("Agg_Trans_full.csv")
# Agg_Trans.to_csv(file_path, index=False)
# print("✅ CSV saved at:", file_path)
# --- Step 2: MySQL Connection with SQLAlchemy ---
engine = create_engine("mysql+mysqlconnector://2DE1JXrXsdMy4D9.root:152Td1rnHqf2yQs3@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/Phonepe")

# --- Step 3: Store DataFrame into MySQL ---
Agg_Trans.to_sql("transactions", con=engine, if_exists="replace", index=False)

# --- Step 4: Fetch Back Data ---
df_mysql = pd.read_sql("SELECT * FROM transactions", con=engine)
# df_mysql.to_csv("From_db.csv",index=False)
# filepath1= os.path.abspath("From_db.csv")
# df_mysql.to_csv(filepath1,index=False)
# print("from db",filepath1)


# --- Step 5: Streamlit Dashboard ---
def main():
    st.title("📊 PhonePe Pulse - Transaction Dashboard")

    df = df_mysql
    with open("india.geojson", "r") as f:
        india_states = json.load(f)


    # Sidebar filters
    st.sidebar.header("Filters")
    state = st.sidebar.selectbox("Select State", sorted(df["State"].unique()))
    year = st.sidebar.selectbox("Select Year", sorted(df["Year"].unique()))
    quarter = st.sidebar.selectbox("Select Quarter", sorted(df["Quarter"].unique()))
    transaction_type = st.sidebar.selectbox("Select Type", sorted(df["Transaction_type"].unique()))

    # Apply filters
    filtered_df = df[
        (df["State"] == state) & 
        (df["Year"] == year) & 
        (df["Quarter"] == quarter)
    ]
    state_mapping = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar Islands",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra and Nagar Haveli and Daman and Diu",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"
    }
    filtered_df["State"] = filtered_df["State"].replace(state_mapping)
    # Aggregate at State Level
    state_summary = filtered_df.groupby("State").agg({
        "Transaction_amount": "sum",
        "Transaction_count": "sum"
    }).reset_index()
 # --- Step 5: Plotly Choropleth Map ---
    fig = px.choropleth(
        state_summary,
        geojson=india_states,
        featureidkey="properties.ST_NM",  # depends on geojson property key
        locations="State",
        color="Transaction_amount",
        hover_name="State",
        hover_data={"Transaction_amount": True, "Transaction_count": True},
        color_continuous_scale="Viridis",
        title=f"💰 Transaction Amount across States ({year} - Q{quarter} - {transaction_type})"
    )

    fig.update_geos(fitbounds="locations", visible=False)

    st.plotly_chart(fig, use_container_width=True)

    st.subheader(f"Transactions in {state} - {year} Q{quarter}")
    st.dataframe(filtered_df)

    # Charts
    st.subheader("💰 Transaction Amount by Type")
    st.bar_chart(filtered_df.set_index("Transaction_type")[["Transaction_amount"]])

    st.subheader("🧾 Transaction Count by Type")
    st.bar_chart(filtered_df.set_index("Transaction_type")["Transaction_count"])

    # Line Chart - Growth
    state_growth = df.groupby(["Year","State"])["Transaction_amount"].sum().reset_index()
    fig1 = px.line(
        state_growth,
        x="Year",
        y="Transaction_amount",
        color="State",
        title="📈 State-Level Growth in Transactions"
    )
    st.plotly_chart(fig1)

if __name__ == "__main__":
    main()
