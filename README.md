# Facebook data collection system

The initial write-up and specification can be found [in the Operam drive](https://drive.google.com/drive/folders/1zrGT3l3BbLmdhs6BwlL8CUEViqFXrJgF?usp=sharing).

## Prerequisites

This project relies on Make, [Docker and Docker Compose](https://www.docker.com/docker-mac). Please ensure you have the latest version of all 3 tools installed before you begin development.

In order to build the project locally, you will need to be added to the [Operam Docker Hub Organization](https://hub.docker.com/u/operam/). If you are not currently added, please post a message in the #engineering Slack channel with your Docker Hub username and Mike, JJ or Peto will get you setup.

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

