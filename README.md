# BigScout

BigScout is an AI-powered football scouting system that leverages big data technologies and OpenAI's GPT models to help users find players that match their tactical and strategic needs. With just a simple prompt (e.g., *"box-to-box midfielder under 21"*), BigScout scans massive datasets, identifies top-performing players, and explains its reasoning — just like a human scout would.

---

## 🧑‍💻 Authors
Umut Eray Açıkgöz
Ulaş Uçrak
Bülent Batıkan Sarıkaya  

---

## 📸 Demo Screenshots

### User Interface
![UI Screenshot 1](./flaskUI-2.png)
![UI Screenshot 2](./Ekran%20g%C3%B6r%C3%BCnt%C3%BCs%C3%BC%202025-05-17%20181018.png)

---

## 🔍 Features

- 🧠 Semantic player search using GPT embeddings
- 📈 Live data ingestion from [FBref](https://fbref.com/)
- ⚙️ Real-time ETL processing with Apache Spark
- 🧰 Distributed architecture using Kafka and Zookeeper
- 🗃️ NoSQL storage using MongoDB
- 🖥️ Minimal Flask frontend for prompt interaction
- 🧪 Detailed player reasoning, stats, and comparisons

---

## ⚙️ Technologies Used

- **Programming Language**: Python  
- **Frameworks / Libraries**: Flask, scikit-learn, OpenAI API  
- **Big Data Tools**: Apache Kafka, Apache Spark, Apache Zookeeper  
- **Database**: MongoDB (visualized with MongoDB Compass)  
- **Web Scraping**: Selenium  
- **Embedding & ML**: OpenAI GPT-based embeddings, pandas, NumPy  

---

## 🚀 Setup Instructions

### 🐳 Prerequisites

- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [MongoDB Compass (optional for UI)](https://www.mongodb.com/products/compass)
- OpenAI API key (set it in `.env`)

### 🔧 Step-by-step Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/bigscout.git
   cd bigscout
Start Kafka, Zookeeper, MongoDB via Docker



📦 Data Sources & APIs
FBref.com
Source of raw player statistics and seasonal performance data.

OpenAI API
Used for embedding player data and generating scout-style reasoning.

📋 License
This project is licensed under the MIT License. See the LICENSE file for details.
