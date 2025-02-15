# Streak Calculator

## Overview
📊 Streak Calculator is a Python script that connects to Notion database tables to calculate and track streaks based on given criteria. It helps users monitor their consistency in various activities logged in Notion.

## Features
- 🔗 **Notion Integration**: Connects seamlessly with Notion database tables.
- 📅 **Streak Calculation**: Automatically determines streaks based on user-defined criteria.
- ⚡ **Automation Friendly**: Can be run manually or scheduled via cron jobs or task schedulers.
- 📤 **Customizable Output**: Provides streak results that can be used in dashboards or reports.

## Installation

### Prerequisites
Ensure you have the following installed:
- 🐍 Python 3.8+
- 🔑 A Notion API token
- 📂 A Notion database set up with relevant entries

### Setup Steps
1. 📥 **Clone the repository:**
   ```bash
   git clone https://github.com/Lakendary/Streak-Calculator.git
   cd Streak-Calculator
   ```

2. 🛠 **Create and activate a virtual environment (optional but recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. 📦 **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. ⚙️ **Configure environment variables:**
   - Create a `.env` file in the project directory and add:
     ```plaintext
     NOTION_API_KEY=your_notion_api_key_here
     NOTION_DATABASE_ID=your_notion_database_id_here
     ```

5. 🚀 **Run the script:**
   ```bash
   python streak_calculator.py
   ```

## Usage
- 📊 **Track Streaks**: Run the script to calculate and display your streaks.
- 🕒 **Automate**: Schedule the script using cron jobs (Linux/macOS) or Task Scheduler (Windows).
- 🔄 **Customize**: Modify the script to fit your Notion setup and tracking needs.

## Contributing
Contributions are welcome! If you’d like to add new features or fix bugs:
1. 🍴 Fork the repository.
2. 🌿 Create a new branch (`git checkout -b feature-branch`).
3. 📝 Commit your changes (`git commit -m "Add new feature"`).
4. 📤 Push to your branch (`git push origin feature-branch`).
5. 🔄 Open a pull request.

## License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Contact
For issues or feature requests, open an issue on GitHub: [Streak Calculator](https://github.com/Lakendary/Streak-Calculator/issues)

