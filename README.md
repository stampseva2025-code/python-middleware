```markdown
# 🚀 StampSeva 2025 - Python Backend API

Welcome to the backend repository for **StampSeva 2025**! This project is a robust, Flask-based REST API designed to serve as the cloud infrastructure for a massive postage stamp digitization and archiving system. 

It handles complex data synchronization between local Electron/React clients and a central MySQL database, features an advanced perceptual hashing engine for duplicate detection, and manages sheet lifecycles and user authentication.

## ✨ Key Features

* **Advanced Duplicate Detection:** Utilizes Bitwise XOR calculations in SQL to compare Perceptual (pHash), Difference (dHash), and Wavelet (wHash) image fingerprints to instantly detect duplicate stamps across a massive database.
* **Smart Sync Engine:** Includes endpoints to receive bulk chunk data from local clients, utilizing `ON DUPLICATE KEY UPDATE` to seamlessly merge offline work with the cloud.
* **Sheet Versioning (The Waterfall):** Automatically manages up to 5 historical versions of sheet images, shifting URLs sequentially when new edits are uploaded.
* **Secure Authentication:** Features role-based access control, `flask-bcrypt` password hashing, and API Key validation for all administrative and synchronization routes.
* **Dynamic Dashboard Analytics:** Aggregates real-time statistics on operator throughput, AI-enrichment progress, theme heatmaps, and global sheet lifecycles.

## 🛠️ Tech Stack
* **Language:** Python 3.x
* **Framework:** Flask
* **Database:** MySQL (via `mysql-connector-python`)
* **Security:** `flask-bcrypt`, `flask-cors`
* **Hosting Integration:** Ready for local development, PythonAnywhere, or any WSGI-compatible server.

---

## 🚀 How to Setup and Run Locally

Follow these steps to run the Flask server on your local machine.

### 1. Clone the Repository
```bash
git clone [https://github.com/YOUR-USERNAME/YOUR-REPO-NAME.git](https://github.com/YOUR-USERNAME/YOUR-REPO-NAME.git)
cd YOUR-REPO-NAME
```

### 2. Install Dependencies
Make sure you have Python installed. It is recommended to use a virtual environment. Install the required libraries by running:
```bash
pip install Flask mysql-connector-python flask-bcrypt flask-cors
```

### 3. Configure the Database and Security
Open the main Python file and locate the configuration sections. **For security, ensure you update these values** before deploying or sharing the code:
* `SHARED_SECRET_KEY`: Set your secret API key used by the frontend to authenticate requests.
* `db_config`: Update the `host`, `user`, `password`, and `database` fields to point to your local or cloud MySQL instance.

### 4. Start the Server
Run the Flask development server:
```bash
flask --app your_filename.py run --debug
# OR simply run: python your_filename.py (if you add app.run() to the bottom of the script)
```
The API will be available at `http://127.0.0.1:5000`.

---

## ☁️ How to Deploy on PythonAnywhere

This code is perfectly structured to run on [PythonAnywhere](https://www.pythonanywhere.com/).

1. Log in to your PythonAnywhere account.
2. Go to the **Web** tab and click **Add a new web app**.
3. Choose **Flask** and select your desired Python version.
4. Upload this Python file to your server via the **Files** tab.
5. Open a **Bash Console** and install the required dependencies:
   ```bash
   pip3 install mysql-connector-python flask-bcrypt flask-cors --user
   ```
6. Go to the **Web** tab, open the `WSGI configuration file`, and ensure it imports your Flask `app` object correctly.
7. Click the green **Reload** button. Your API is now live!

---

## 🔒 Security & Access
All protected routes require an `X-API-KEY` header matching the `SHARED_SECRET_KEY` configured in the server. Operator login routes verify user credentials against `bcrypt` hashes stored in the MySQL database.
```