from datetime import datetime, date
from facebook_business.adobjects.adsinsights import AdsInsights

from common.enums.entity import Entity
from common.enums.reporttype import ReportType
from common.id_tools import generate_universal_id, universal_id_fields
from common.tztools import dt_to_other_timezone


_entity_type_id_field_map = {
    Entity.Campaign: AdsInsights.Field.campaign_id,
    Entity.AdSet: AdsInsights.Field.adset_id,
    Entity.Ad: AdsInsights.Field.ad_id,
}


def _from_non_segmented_entity(data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated only by entity ID

    :param entity_type:
    :return:
    """
    assert entity_type

    entity_id = data[_entity_type_id_field_map[entity_type]]
    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            entity_id=entity_id,
            entity_type=entity_type,
            **kwargs
        ),
        'entity_id': entity_id,
        'entity_type': entity_type
    }


_date_start = 'date_start'
_date_stop = 'date_stop'
_hourly_stats_aggregated_by_advertiser_time_zone = 'hourly_stats_aggregated_by_advertiser_time_zone'


def _from_day_segmented_entity(data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated by entity ID and reporting date

    :param str timezone_name: Intentionally used first to bind it so it's passed in only once.
    :param dict entity_type:
    :return:
    """

    assert entity_type

    # ...
    # "account_id": "2034428216844013",
    # "ad_id": "23842698250300224",
    # "adset_id": "23842698250720224",
    # "campaign_id": "23842698250110224",
    # "clicks": "0",
    # "cpc": "0",
    # "cpm": "5",
    # "ctr": "0",
    # "date_start": "2017-12-31", <--------------------
    # "date_stop": "2017-12-31",
    # "impressions": "2",
    # "reach": "2",
    # "spend": "0.01"
    # ...

    entity_id = data[_entity_type_id_field_map[entity_type]]

    # let's be lazy here and assume we always get single day data
    # so we'll ignore date_stop for now.

    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            fields=universal_id_fields,
            entity_id=entity_id,
            entity_type=entity_type,
            range_start=data[_date_start],
            **kwargs
        ),
        'range_start': data[_date_start],
        'entity_id': entity_id,
        'entity_type': entity_type
    }


def _from_hour_segmented_entity(timezone_name, data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated only by entity ID

    :param str timezone_name: Intentionally used first to bind it so it's passed in only once.
    :param dict entity_type:
    :return:
    """

    assert timezone_name
    assert entity_type

    # ...
    # "ctr": "0",
    # "date_start": "2017-12-31",
    # "date_stop": "2017-12-31",
    # "frequency": "0",
    # "hourly_stats_aggregated_by_advertiser_time_zone": "00:00:00 - 00:59:59",
    # "impressions": "371",
    # ...

    entity_id = data[_entity_type_id_field_map[entity_type]]

    # let's be lazy here and assume we always get single day data
    # so we'll ignore date_stop for now.

    date_str = data[_date_start]
    hour_str = data[_hourly_stats_aggregated_by_advertiser_time_zone].split('-', 1)[0].strip()
    dt = datetime.strptime(date_str + 'T' + hour_str, '%Y-%m-%dT%H:%M:%S')
    dt_as_utc = dt_to_other_timezone(dt, 'UTC', timezone_name)

    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            entity_id=entity_id,
            entity_type=entity_type,
            range_start=dt_as_utc,
            **kwargs
        ),
        # Note that we communicate UTC-based range_start
        # which will be different in date and time from
        # data['date_start'] value and hour range attribute value
        # in Facebook's original data.
        'range_start': dt_as_utc.strftime('%Y-%m-%dT%H:%M:%S'),
        'entity_id': entity_id,
        'entity_type': entity_type
    }


def _from_age_gender_segmented_entity(data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated only by entity ID

    :param str timezone_name: Intentionally used first to bind it so it's passed in only once.
    :param dict entity_type:
    :return:
    """

    assert entity_type

    # ...
    # "date_start": "2018-06-02",
    # "date_stop": "2018-06-02",
    # "age": "18-24",  # <----------
    # "clicks": "10",
    # "cpc": "0.117",
    # "cpm": "2.521552",
    # "cpp": "2.526998",
    # "ctr": "2.155172",
    # "gender": "female",  # <----------
    # "impressions": "464",
    # ...

    entity_id = data[_entity_type_id_field_map[entity_type]]

    # let's be lazy here and assume we always get single day data
    # so we'll ignore date_stop for now.

    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            fields=universal_id_fields + ['age','gender'],
            entity_id=entity_id,
            entity_type='A',
            range_start=data[_date_start],
            age=data['age'],
            gender=data['gender'],
            **kwargs
        ),
        'range_start': data[_date_start],
        'entity_id': entity_id,
        'entity_type': entity_type
    }


def _from_platform_segmented_entity(data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated only by entity ID

    :param str timezone_name: Intentionally used first to bind it so it's passed in only once.
    :param dict entity_type:
    :return:
    """

    assert entity_type

    # ...
    # "date_start": "2018-06-02",
    # "date_stop": "2018-06-02",
    # "ctr": "1.88383",
    # "frequency": "1.001572",
    # "impressions": "637",
    # "platform_position": "feed",  # <-----------
    # "publisher_platform": "facebook",  # <-----------
    # "reach": "636",
    # ...

    entity_id = data[_entity_type_id_field_map[entity_type]]

    # let's be lazy here and assume we always get single day data
    # so we'll ignore date_stop for now.

    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            fields=universal_id_fields + ['publisher_platform','platform_position'],
            entity_id=entity_id,
            entity_type=entity_type,
            range_start=data[_date_start],
            publisher_platform=data['publisher_platform'],
            platform_position=data['platform_position'],
            **kwargs
        ),
        'range_start': data[_date_start],
        'entity_id': entity_id,
        'entity_type': entity_type
    }


def _from_dma_segmented_entity(data, entity_type=None, **kwargs):
    """
    Generates Universal record ID from data that is
    differentiated only by entity ID

    :param str timezone_name: Intentionally used first to bind it so it's passed in only once.
    :param dict entity_type:
    :return:
    """

    assert entity_type

    # ...
    # "account_id": "2034428216844013",
    # "ad_id": "23842698250300224",
    # "adset_id": "23842698250720224",
    # "campaign_id": "23842698250110224",
    # "clicks": "0",
    # "cpc": "0",
    # "cpm": "5",
    # "ctr": "0",
    # "date_start": "2017-12-31",
    # "date_stop": "2017-12-31",
    # "dma": "Macon",  <--------------------- id
    # "impressions": "2",
    # "reach": "2",
    # "spend": "0.01"
    # ...

    entity_id = data[_entity_type_id_field_map[entity_type]]

    # let's be lazy here and assume we always get single day data
    # so we'll ignore date_stop for now.

    # The rest of data is in kwargs
    return {
        'id': generate_universal_id(
            fields=universal_id_fields + ['dma'],
            entity_id=entity_id,
            entity_type=entity_type,
            range_start=data[_date_start],
            dma=data['dma'],  # generate_universal_id URL-encodes values. Sleep well.
            **kwargs
        ),
        'range_start': data[_date_start],
        'entity_id': entity_id,
        'entity_type': entity_type
    }


report_type_vendor_data_extractor_map = {
    ReportType.day: _from_day_segmented_entity,
    ReportType.day_age_gender: _from_age_gender_segmented_entity,
    ReportType.day_dma: _from_dma_segmented_entity,
    # Hour handler is special. Needs Timezone name as first arg
    # It will be handled specially by code that matches report types
    # to vendor ID handlers
    ReportType.day_hour: _from_hour_segmented_entity,
    ReportType.day_platform: _from_platform_segmented_entity,
    ReportType.lifetime: _from_non_segmented_entity,
}
