# Create a virtual environment named 'venv'
python3 -m venv venv

# Activate the virtual environment
# For macOS/Linux:
source venv/bin/activate
# For Windows:
# venv\Scripts\activate

# Install required packages from requirements.txt
pip install -r requirements.txt

# Run the script with Python 3
python3 mqtt_mesh_bridge.py

# Deactivate the virtual environment (optional) when finished
deactivate
