# Spotify Analytics Pipeline with AWS, Snowflake, & Power BI

A serverless data pipeline designed to extract data from multiple playlists from Spotify every 8 hours, transform it into analytical datasets, and load it into Snowflake for business analytics and visualization in Power BI.

---

## **Architecture Overview**
### **Solution Components**
- **Data Source:** Spotify API (Multiple Playlists)
- **Extraction:** AWS Lambda + CloudWatch trigger (8 Hours)
- **Storage:** AWS S3 (Raw JSON & Transformed CSV)
- **Transformation:** AWS Lambda
- **Data Warehouse:** Snowflake
- **Data Loading:** Snowpipe
- **Visualization:** Power BI

---

## **Implementation Details**

### **Data Extraction**
- Every 8 hours, CloudWatch triggers invoke an AWS Lambda function.
- Lambda connects to Spotify API using environment variables for authentication.
- Raw data is stored in S3: `spotify-etl-pipeline-sn/raw_data/to_be_processed/`.

### **Data Transformation**
- S3 put events trigger another Lambda function for transformation.
- Data is normalized into three entities:
    - **Songs:** Metrics like duration, popularity, added date.
    - **Artists:** Name, URLs.
    - **Albums:** Release dates, total tracks.
- Transformed data is stored as CSV files in dedicated S3 directories.

---

## **Process Flow**

### Step-by-Step Process Flow
1. **Data Extraction**
   - CloudWatch triggers the `spotify_api_data_extract` Lambda function every 8 hours.
   - The Lambda function connects to Spotify API to extract data from multiple playlists.
   - The raw data is stored in the S3 bucket `spotify-etl-pipeline-sn/raw_data/to_be_processed/`.

2. **Data Transformation**
   - A new file in the `raw_data` bucket triggers the `spotify_data_api_load` Lambda function.
   - The function processes and normalizes data into three entities: `Songs`, `Artists`, and `Albums`.
   - Transformed data is stored in the S3 bucket `spotify-etl-pipeline-sn/transform_data/` in respective folders.

3. **Data Loading**
   - Snowpipe automatically ingests transformed data from the `transform_data` bucket into Snowflake tables.
   - Tables include `Songs`, `Artists`, and `Albums`.

4. **Data Visualization**
   - Views are created in Snowflake to organize the data for analytics.
   - Power BI connects to Snowflake and visualizes the data for business insights.

5. **Machine Learning Integration**
   - Data from Snowflake is accessed using Jupyter Notebook for ML tasks.
   - Preprocessing, feature engineering, and Random Forest model training are performed.
   - The trained model predicts song popularity, aiding playlist curation and enhancing decision-making for playlist management.

---

## **Data Model**

### **Tables**
1. **Songs**
    - `song_id (PK)`
    - `name`
    - `duration_ms`
    - `url`
    - `popularity`
    - `added_date`
    - `album_id (FK)`
    - `artist_id (FK)`

2. **Artists**
    - `artist_id (PK)`
    - `name`
    - `url`

3. **Albums**
    - `album_id (PK)`
    - `name`
    - `release_date`
    - `total_tracks`
    - `url`

---

## **Analytics Views**
### Key Metrics
- **Artist Analytics:**
    - Top 5 artists by song count.
    - Average popularity, song duration, and track appearances.
- **Popularity Trends:**
    - Recent releases vs. older tracks.
    - Current top songs by popularity.

---

## **Business Intelligence**
### Power BI Insights:
- Artist performance metrics.
- Song popularity trends.
- Release timing and track metrics distribution.

### Machine Learning Model:
- **Purpose:** Predicts the popularity of songs based on historical and current data.
- **Workflow:**
    - Data is preprocessed and relevant features are engineered.
    - A Random Forest model is trained using song features such as duration, popularity, and artist-related attributes.
    - The model provides predictions for song popularity, enabling data-driven decisions for playlist optimization.
- **Impact:** Helps identify potential hit songs and optimize playlist strategies to maximize listener engagement.

---

## **Tech Stack**
- **AWS Services:** Lambda, S3, CloudWatch, IAM.
- **Data Warehouse:** Snowflake.
- **BI Tool:** Power BI.
- **Python Libraries:** `spotipy`, `pandas`, `boto3`.

---

## **Key Learnings**
- Serverless ETL pipeline design with AWS and Snowflake.
- Data modeling for analytical workloads.
- Power BI dashboard creation and SQL analytics.
- Machine learning model development and integration with ETL pipelines.

---

## **Future Enhancements**
- Broaden analysis with additional playlists.
- Implement data quality checks.
- Add predictive analytics and historical trend analysis.