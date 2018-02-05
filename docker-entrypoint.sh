#!/bin/bash

# inspired by
# https://denibertovic.com/posts/handling-permissions-with-docker-volumes/

USER_ID="${USER_ID:-$(id -u)}"
GROUP_ID="${GROUP_ID:-$(id -g)}"

if [ $USER_ID = "0" ] || [ $GROUP_ID = "0" ]; then
  # it's root. no point messing with user
  exec "$@"
else
  useradd --shell /bin/bash -u $USER_ID -g $GROUP_ID -o -c "" -m user
  export HOME=/home/user
  exec /usr/local/bin/gosu user "$@"
fi
