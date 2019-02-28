def convert_class_with_props_to_str(class_instance):
    new_dict = {
        key: class_instance.__dict__[key]
        for key
        in class_instance.__dict__
        if key[:2] != '__' and not callable(class_instance.__dict__[key])
    }

    return f'<{class_instance.__class__.__name__} {new_dict}>'
