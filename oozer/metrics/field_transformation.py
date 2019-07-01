from typing import Dict, Iterable


class FieldTransformation:
    @classmethod
    def _remap_actions(cls, actions_dict: Dict) -> Dict:
        # Because of "offsite_conversion.fb_pixel_view_content" action types and similar
        action_type = actions_dict['action_type']
        other_keys = set(actions_dict.keys()).difference({'action_type'})

        out_dict = {}
        for key in other_keys:
            out_dict[key] = actions_dict[key]

        return {action_type: out_dict}

    @classmethod
    def transform(cls, datum: Dict, action_fields: Iterable[str]) -> Dict:
        transformed = {}

        for action_field_name in action_fields:
            if action_field_name in datum:
                remapped_actions_dict = {}
                for actions in datum[action_field_name]:
                    remapped_actions_dict.update(**FieldTransformation._remap_actions(actions))

                transformed[action_field_name] = remapped_actions_dict

        return {**datum, '__transformed': transformed}
