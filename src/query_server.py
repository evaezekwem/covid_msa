from flask import Flask
import pandas as pd
from pandasql import sqldf
import os

app = Flask(__name__)

cases_path = os.path.join(os.pardir,'data','all_msas_cases.csv')
deaths_path = os.path.join(os.pardir,'data','all_msas_deaths.csv')
# @app.route('/')
# def index():
#     return "This is the homepage"

@app.route('/cases')
def cases():
    df = pd.read_csv(cases_path)
    return sqldf("SELECT * FROM df").to_json()

@app.route('/deaths')
def deaths():
    df = pd.read_csv(deaths_path)
    return sqldf("SELECT * FROM df").to_json()


if __name__=="__main__":
    app.run(port=3444, debug=True)