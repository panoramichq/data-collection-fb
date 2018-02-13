from common.celeryapp import get_celery_app, RoutingKey

app = get_celery_app()


@app.task(routing_key=RoutingKey.longrunning)
def echo(message='This is Long-Running queue'):
    print(message)
