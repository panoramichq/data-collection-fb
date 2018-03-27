import config.application


_vendor_data_attr_name = f'__{config.application.UNIVERSAL_ID_COMPONENT_VENDOR}'


def add_vendor_data(data, **vendor_params):
    """
    :param dict data: Data dict to be augmented with vendor data block
    :param id:
    :param vendor_params:
    :return:
    """
    data[_vendor_data_attr_name] = vendor_data = data.get(_vendor_data_attr_name, {})
    vendor_data.update(vendor_params)
    return data
