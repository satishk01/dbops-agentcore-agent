"# dbops-agentcore-agent" 

Files are in the root folder only

####Install and configure python 3.12

curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv --python 3.12

###Activate the environment
source .venv/bin/activate

####instal all the packages 
uv pip install -r requirements.txt

####Deploy the agent 
python3 ./deploy_dataops_agent.py

####Test the agent
streamlit run dataops_ui.py
