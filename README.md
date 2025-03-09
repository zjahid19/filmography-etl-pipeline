# ETL Pipeline with Pandas

This project extracts data from wikipedia, transforms it using pandas, and loads it into an postgres database table.

## 📌 Features
- Extracts data from:
    - Scrape data from wikipedia
- Performs data transformations using **pandas**
- Loads transformed data into **Postgres Table**
- Uses **object-oriented programming (OOP)** concepts

---

## 🚀 Getting Started

Follow these steps to set up and run the project on your local machine.

### 1️⃣ **Clone the Repository**
```bash
git clone https://github.com/zjahid19/filmography-etl-pipeline.git
cd filmography-etl-pipeline


python -m venv venv
# Activate it:
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

pip install -r requirements.txt