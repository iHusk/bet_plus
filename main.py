from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import GCS
import pandas as pd
import io
import requests

URL = "https://www.thepredictiontracker.com/ncaapredictions.csv"
## Config is stored on prefect cloud
# GCS_BLOCK = GCS.load("python-predictions")
NCAA_PATH = "/home/admin/python/prefect/bet_plus/preds/preds.csv"

#@sync_compatible
#async def put_predictions(self):
#    print("Writing preds.csv ...")
#    return await self.filesystem.put_directory(local_path=NCAA_PATH)


@task(retries=2, retry_delay_seconds=10)
def get_predictions():
    print("Downloading ...")
    page = requests.get(URL).content
    preds = pd.read_csv(io.StringIO(page.decode('utf-8')))
    print(preds.head(5))
    return preds

@task
def save_predictions(df, path):
    print("Saving ...")
    with open(path, 'w') as csvfile:
        df.to_csv(path_or_buf=csvfile)


@flow(task_runner=SequentialTaskRunner)
def test_retries():
    print("Getting predictions ...")
    preds = get_predictions()
    save_predictions(preds, NCAA_PATH)
#    put_predictions()

if __name__ == "__main__":
    test_retries()
