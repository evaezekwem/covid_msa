from flask import Flask
# from flask_cors import CORS
import pandas as pd
from pandasql import sqldf
import os

app = Flask(__name__)
# CORS(app)

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
#     app.run(host="0.0.0.0", port=3444, debug=True)
    app.run()
