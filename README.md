# Facebook data collection system

This contains a system which can be used to collect data from Facebook Ad accounts. It takes into account rate limits and balances them to ensure you always have the most recent data. It relies on many AWS services to operate.

## Prerequisites

This project relies on Make, [Docker and Docker Compose](https://www.docker.com/docker-mac). Please ensure you have the latest version of all 3 tools installed before you begin development.

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

We use `pip-tools` to manage our dependencies. The dependencies are divided into 3 files based on how they are used and how frequently they are expected to change. Please use the following when deciding where to place dependencies, keeping in mind that they are only suggestions:

When adding a new project-wide dependency which is from a stable Egg or is used in the majority of the code, place them in `requirements.base.src`. Changes to this file trigger a rebuild of the entire dependency chain, and therefore increase the time it takes to build, deploy and test.

When adding a dependency used in a module, or one that is expected to change often (for example, the Facebook Ads SDK), put them in the `requirements.src` file.

When adding dependencies needed for testing or local development, place them in the `requirements.dev.src` file. This file is typically used for very new features. It is expected that it will change frequently, and that once a feature has reached a stable state the Eggs will move into one of the other files.

#### Compiling dependencies

To compile the dependencies run `make requirements-compile`, this runs the `pip-compile` on the requirement file chain. As we run pip install when building images, to rebuild the packages quit the stack and run `make image` to rebuild the image or run `pip install -r requirements.txt` directly in the container (you should also be able to update the packages with [`pip-sync`](https://github.com/jazzband/pip-tools#example-usage-for-pip-sync)).
