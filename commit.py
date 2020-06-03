import subprocess
import datetime.datetime as dt

subprocess.call('git add .', shell=True)
# time.sleep(3)
subprocess.call(f'git commit -m "Data updated as at {dt.now().strftime("%d-%m-%Y")}"', shell=True)
# time.sleep(3)
subprocess.call('git push', shell=True)
