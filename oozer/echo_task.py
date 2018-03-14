from common.celeryapp import get_celery_app, RoutingKey
from common.measurement import Measure

app = get_celery_app()


@app.task
@Measure.timer(__name__, function_name_as_metric=True)
@Measure.counter(__name__, function_name_as_metric=True, count_once=True)
def echo(message='This is default queue'):
    print(message)
