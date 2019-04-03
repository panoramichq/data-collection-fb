from common.util import redact_access_token


def test_clear_secrets_match():
    message = """

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v3.1/12345/insights?access_token=sdlkandlk12j4lkhjrlk1nklfalknfslai1oi2j341lknralkdhop1i2&limit=25&after=NTE5OQZDZD
  Params:  {}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "An unknown error occurred",
        "error_subcode": 99
      }
    }
"""
    result = redact_access_token(Exception(message))

    assert str(result) == """

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v3.1/12345/insights?access_token=********************************************************&limit=25&after=NTE5OQZDZD
  Params:  {}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "An unknown error occurred",
        "error_subcode": 99
      }
    }
"""


def test_clear_secrets_no_match():
    message = """

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v3.1/12345/insights?limit=25&after=NTE5OQZDZD&params=test1,test2
  Params:  {}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "An unknown error occurred",
        "error_subcode": 99
      }
    }
"""
    result = redact_access_token(Exception(message))

    assert str(result) == """

  Message: Call was not successful
  Method:  GET
  Path:    https://graph.facebook.com/v3.1/12345/insights?limit=25&after=NTE5OQZDZD&params=test1,test2
  Params:  {}

  Status:  500
  Response:
    {
      "error": {
        "code": 1,
        "message": "An unknown error occurred",
        "error_subcode": 99
      }
    }
"""
