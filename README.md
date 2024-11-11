# Spotify ETL Pipeline with Snowflake & AWS

A serverless data pipeline designed to extract Spotify's Top 100 playlist data weekly, transform it into analytical datasets, and load it into Snowflake for business analytics and visualization in Power BI.

---

## **Architecture Overview**
### **Solution Components**
- **Data Source:** Spotify API (Top 100 Playlist)
- **Extraction:** AWS Lambda + CloudWatch trigger (weekly)
- **Storage:** AWS S3 (Raw JSON & Transformed CSV)
- **Transformation:** AWS Lambda
- **Data Warehouse:** Snowflake
- **Data Loading:** Snowpipe
- **Visualization:** Power BI

---

## **Implementation Details**

### **Data Extraction**
- Weekly CloudWatch triggers invoke an AWS Lambda function.
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

---

## **Future Enhancements**
- Broaden analysis with additional playlists.
- Implement data quality checks.
- Add predictive analytics and historical trend analysis.
