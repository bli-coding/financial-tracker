# Personal Financial Tracker

This repository hosts a proof-of-concept (POC) designed to experiment with automating personal financial data ingestion using the Plaid API, storing normalized records in an open table format (Delta/Iceberg), and exposing the curated data to a lightweight BI layer.

## Repository Layout

- `docs/` – planning artifacts, architecture sketches, and experiment notes.
- `config/` – example environment files and configuration templates.
- `src/` – source code grouped by responsibility (sandbox experiments, ingestion, storage, orchestration, analytics).
- `infra/` – infrastructure as code or container definitions when you are ready to deploy repeatably.

## Quick Start

1. Duplicate `.env.template` as `.env` and populate Plaid + storage credentials.
2. Follow `docs/roadmap.md` to implement each milestone incrementally.
3. Use the `sandbox` module to validate Plaid connectivity before persisting data.
4. Promote the validated logic into `ingestion` and `storage` modules, then automate via `orchestration`.
5. Connect your BI tool (Metabase or similar) using the views/materialized tables generated in `analytics`.

The repository currently contains stubs and documentation so you can grow the project deliberately without mixing planning and execution artifacts.




quick setup env
1. python3 --version
2. create env: python -m venv .venv
3. activate env: source .venv/bin/activate
4. java -version
5. download latest version: brew install openjdk@17
6. set path for MacOS to find Java
    echo 'export PATH="/usr/local/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
    echo 'export CPPFLAGS="-I/usr/local/opt/openjdk@17/include"' >> ~/.zshrc
7. validate java : java -version


create project venv with Poetry
1. install pipx
    brew install pipx
    pipx ensurepath
2. install poetry: pipx install poetry
3. check version: poetry --version
5. create a new project: poetry new financial-tracker-demo 
6. designate which python to use in poetry env
    ls /opt/homebrew/bin/python* (pick one version)
    poetry env use /opt/homebrew/bin/python3.14
7. retrieve the venv activate command
    poetry env activate
    copy paste the command line and activate env


Add core dependencies
1. after activing the env, add Plaid + HTTP: poetry add plaid-python python-dotenv httpx
2. add data engineering and data toolings: poetry add duckdb deltalake pyspark pandas polars delta-spark
3. Create packages in a dedicated dev group: poetry add --group dev ruff mypy pytest pytest-cov
4. show the dependency tree: poetry show --tree
5. Create ruff.toml, mypy.ini, pytest.ini rules
6. add Makefile (optional)



Set up .env management
1. create credential template: config/.env.template
2. create .secret outside of the project and put in the sensitive info: 
    mkdir -p ~/.secrets
    chmod 700 ~/.secrets
    touch ~/.secrets/financial-tracker.env
3. install direnv: brew install direnv
4. hook to shell: echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc
    remember to reload shell config
5. create .envrc which tells direnv everytime when you cd into the project: touch .envrc
    unblock restriction: direnv allow
    then fill in path where credentials from



Setup sandbox credentials and clone quickstart
1. Retrieve client ID and secrets from Plaid dashboard and save it on local .secrets
2. clone repo quickstart
3. copy .env sample
4. create a .vnerc to auto import variables into .env
    quick check if import succeeded: env | grep PLAID
5. run backend: 
    cd ~/repos/quickstart
    cd python
    source .venv/bin/activate
    ./start.sh
6. run frontend: cd ~/repos/quickstart/frontend
    npm start
7. make sure you have the certif for HTTPs connection: pip install --upgrade certifi
8. Go through the registeration with Quickstart and you should receive item_id and access_token


