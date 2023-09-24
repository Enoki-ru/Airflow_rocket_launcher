import json
import pathlib
import airflow 
import requests 
import requests.exceptions as requests_exceptions 
from airflow import DAG 
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator

# os.popen('(if not exist tmp (mkdir tmp)) & curl -o tmp/launches.json -L "https://ll.thespacedevs.com/2.0.0/launch/upcoming"')
# pathlib.Path("tmp/images").mkdir(parents=True, exist_ok=True) 
    
def _get_pictures(): # Убеждаемся, что каталог существует 
    pathlib.Path("tmp/images").mkdir(parents=True, exist_ok=True) 
    
    # Скачиваем все изображения в launches.json 
    with open("tmp/launches.json") as f: 
        launches = json.load(f) 
        image_urls = [launch["image"] for launch in launches["results"]] 
        for image_url in image_urls: 
            try: 
                response = requests.get(image_url) 
                image_filename = image_url.split("/")[-1] 
                target_file = f"tmp/images/{image_filename}"
                with open(target_file, "wb") as f: 
                    f.write(response.content) 
                print(f"Downloaded {image_url} to {target_file}") 
            except requests_exceptions.MissingSchema: 
                print(f"{image_url} appears to be an invalid URL.") 
            
            except requests_exceptions.ConnectionError: 
                print(f"Could not connect to {image_url}.")

_get_pictures()