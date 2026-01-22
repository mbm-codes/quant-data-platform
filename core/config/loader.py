import os
import yaml


# ==========================
# Load configuration per environment
# ==========================
def load_config(env: str, config_file):
    env = env or os.getenv("QDP_ENV", "local")  # local/dev/prod
    #config_file = f"./config/ingestion/nse/historical/{env}_nse_historical_load.yaml"
    with open(config_file, "r") as f:
        return yaml.safe_load(f)