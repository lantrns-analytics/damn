import requests

from .base import BaseOrchestratorAdapter

class DagsterAdapter(BaseOrchestratorAdapter):
    def __init__(self, config):
        # Set headers
        self.endpoint = config['endpoint']
        self.headers = {
            "Content-Type": "application/json",
            "Dagster-Cloud-Api-Token": config['api_token'],
        }


    def execute(self, query):
        response = requests.post(
            self.endpoint, # type: ignore
            headers=self.headers, # type: ignore
            json={"query": query}
        )
        
        response.raise_for_status()
        
        return response.json()
