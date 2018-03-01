from common.celeryapp import get_celery_app, RoutingKey

app = get_celery_app()


@app.task
def echo(message='This is default queue'):
    print(message)
