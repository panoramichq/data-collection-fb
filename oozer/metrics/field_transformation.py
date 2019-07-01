from typing import Dict, Iterable


class FieldTransformation:
    @classmethod
    def _remap_actions(cls, field_name: str, actions_dict: Dict) -> Dict:
        # Because of "offsite_conversion.fb_pixel_view_content" action types and similar
        remapped_action_type = actions_dict['action_type'].replace('.', '_')
        base_name = f"{field_name}__{remapped_action_type}"
        base_value = actions_dict.get('value', '')
        other_keys = set(actions_dict.keys()).difference({'action_type', 'value'})

        out_dict = {base_name: base_value}

        for key in other_keys:
            new_key = f"{base_name}_{key}"
            out_dict[new_key] = actions_dict[key]

        return out_dict

    @classmethod
    def transform(cls, datum: Dict, action_fields: Iterable[str]) -> Dict:
        transformed = {}

        for action_field_name in action_fields:
            actions_list = datum.get(action_field_name, [])

            for actions in actions_list:
                transformed.update(
                    **FieldTransformation._remap_actions(field_name=action_field_name, actions_dict=actions)
                )

        return {**datum, '__transformed': transformed}
