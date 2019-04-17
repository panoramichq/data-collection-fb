from unittest import mock

from common.page_tokens import PageTokenManager


@mock.patch('common.page_tokens.get_redis')
@mock.patch('common.page_tokens.PlatformApiContext')
@mock.patch('common.page_tokens.FacebookRequest.execute')
def test_populate_from_scope_entity(mock_execute, mock_api_context, mock_get_redis):
    mock_redis = mock_get_redis.return_value
    mock_entity = mock.Mock(namespace='namespace', platform_tokens=['token1'])
    mock_response = mock.Mock()
    mock_response.json.return_value = {
        'data': [{'id': 'page1-id', 'access_token': 'page1-at'}, {'id': 'page2-id', 'access_token': 'page2-at'}]
    }
    mock_execute.return_value = mock_response
    PageTokenManager.populate_from_scope_entity(mock_entity, 'sweep-id')

    assert mock_redis.zadd.call_args_list == [
        mock.call('fb-sweep-id-page-page1-id-tokens-queue', 'page1-at', 0),
        mock.call('fb-sweep-id-page-page2-id-tokens-queue', 'page2-at', 0),
    ]
