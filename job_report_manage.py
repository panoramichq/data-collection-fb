import gevent
import gevent.pool
import datetime
import json

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.store.jobreport import JobReport
from oozer.common.job_scope import JobScope
from sweep_builder.scorable import _fetch_job_report


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()
        else:
            return super().default(o)


days = ['{0:02d}'.format(n) for n in range(1,32)]
months = ['04', '03']

account_ids = '''1152165094937322
1406145786177930
2031676196920453
2050313278588840
205470093480779
2136762153203416
2170883586509052
220772102169827
360894261127332
42926315
498668387294109
624664164579850
800504646949031
82949321'''.splitlines()


def date_strings_gen():
    for m in months:
        for d in days:
            yield f'2019-{m}-{d}'



def job_ids_gen(report_type=ReportType.day_age_gender):
    for account_id in account_ids:
        for date in date_strings_gen():
            yield JobScope(
                ad_account_id = account_id,
                range_start = date,
                report_type = report_type,
                report_variant = Entity.Ad,
                sweep_id='asdf',
            ).job_id


def get_job_report(tt):
    i, job_id = tt
    print(f"{i} - {job_id}")
    return (
        job_id,
        _fetch_job_report(job_id)
    )

def fetch_all(report_type=ReportType.day_age_gender):
    pool = gevent.pool.Pool(100)
    return pool.imap_unordered(
        get_job_report,
        enumerate(job_ids_gen(report_type=report_type))
    )


def doit(report_type=ReportType.day_age_gender):
    r = []
    for e in fetch_all(report_type=report_type):
        r.append(e)
        gevent.sleep()
    return {
        k:v
        for k, v in r
        if v
    }


def filter_dict(d):
    return {
        k:v
        for k,v in d.items()
        if v is not None
    }


def save_job_reports(fname, report_type=ReportType.day_age_gender):

    vv = list(doit(report_type=report_type).values())
    dd = [filter_dict(v.to_dict()) for v in vv]

    with open(f'{fname}.json', 'w') as fp:
        json.dump(dd, fp, indent=2, sort_keys=1, cls=JSONEncoder)

    return vv


if __name__ == '__main__':
    doit()
