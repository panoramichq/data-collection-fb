# Facebook data collection system

## To get up and running

We use `make` as our "build tool" to run arbitrary commands (for example issue a docker build, start docker-compose of the whole stack etc.)

1. `make image` 
    - builds the images
2. `make pythonuserbase` 
    - links dependencies from the container so we dont need to install local setup
    - this command outputs the path that can be added to IDEs SDK path (to enable autocompletion etc.)
3. `make start-dev`
    - starts the stack with an attached terminal

## How to develop

### Adding dependencies


