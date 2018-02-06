from common.celeryapp import get_celery_app

app = get_celery_app()

@app.task
def display(message='No message'):
    print(message)
