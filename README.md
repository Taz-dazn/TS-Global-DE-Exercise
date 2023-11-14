# Global Data Exercise

#### Prerequisites
1. Make sure your machine runs Python v3.9 (3.6+ should work)
2. Install the latest **pipenv** version
``` shell
pip install pipenv 
```
3. Setup Pipenv locally by running the following command:
``` shell
pipenv install
```
4. Create a **config.yaml** file at the project root to include the following values
``` shell
AWS_ACCESS_KEY_ID: '***'
AWS_SECRET_ACCESS_KEY: '***'
```

## How to run the Script

- Run the following line in your command line from the project root, followed by the batch_date argument:
``` shell
pipenv run python global_data_exercise.py --batch_date=YYYY-MM-DD
```
- YYYY-MM-DD should be replaced by the date you want to run your batch for
- This will execute the script within the pip environment

## How to run the Tests

- Run the following line in your command line from the project root, followed by the batch_date argument:
``` shell
pipenv run python -m pytest test/test_global_data_exercise.py
```
- This will run the tests within the pip environment
