import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

logging.basicConfig(filename='function_app.log', level=logging.INFO)

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def optionsweekday(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')