import logging
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 10 * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def options_weekday_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')