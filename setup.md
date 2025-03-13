# About
WIP - As the new team, we're working on standardizing the environment setup to keep all relevant information in one place and minimize confusion around Python packages, dependencies and env setup. 

<em>**Note**: This document is still in its early phase of development, and there might be mistakes or missing details. If you notice anything that needs updating or clarification, please feel free to contribute or provide feedback.</em>

## Background knowledge about Python package management and environments
- `pyenv` manages multiple Python versions at the system level, allowing you to install and switch between different Python versions for different projects.
- `direnv` automates the loading of environment variables and can auto-activate virtual environments (.venv) when entering a project directory, making workflows smoother.
- `.venv` (created via python -m venv) is Python’s built-in tool for isolating dependencies per project, ensuring that packages don’t interfere with global Python packages.
- `Poetry` is an all-in-one dependency and virtual environment manager that automatically creates a virtual environment (.venv), manages package installations, and locks dependencies (poetry.lock) for reproducibility, making it superior to using pip manually.

## Environment Setup for WSL
1. Install WSL if running on windows and install docker.
2. Open vscode and click the bottom left corner (blue section) then select Connect to WSL with distro. Then select your wsl system, (Ubuntu-24.04).
Once connected, you should see the path as something similar to: `/mnt/d/Source/immunisation-fhir-api/backend`.
3. Run the following commands to install dependencies

```
sudo apt update && sudo apt upgrade -y
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
    libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev \
    liblzma-dev git libgdbm-dev libgdbm-compat-dev
pip install --upgrade pip
```

4. Configure pyenv.

```
pyenv install --list | grep "3.10"
pyenv install 3.10.16 #current latest
```

5. Setup python version in backend folder
```
pyenv local 3.10.16 # Set version in backend (this creates a .python-version file)
```

6. Install poetry
```
pip install poetry

### Point poetry virtual enviornment to .venv
poetry config virtualenvs.in-project true
poetry env use $(pyenv which python)
poetry env info
poetry install
```

7. Create a .env file in the backend folder. Note the variables might change in the future, but it's been copied from the existing README.md from backend. These env variables will be loaded automatically when using direnv

```
AWS_PROFILE=local
DYNAMODB_TABLE_NAME=imms-default-imms-events 
IMMUNIZATION_ENV=local 
```

8. Configure direnv by creating a .envrc file in the backend folder. This points direnv to the .venv created by poetry and loads env variables specified in the .env file

```
export VIRTUAL_ENV=".venv"
PATH_add "$VIRTUAL_ENV/bin"

dotenv
```

9. Restart bash and run `direnv allow`. You should see something similar like: 
```
direnv: loading /mnt/d/Source/immunisation-fhir-api/backend/.envrc
direnv: export +AWS_PROFILE +DYNAMODB_TABLE_NAME +IMMUNIZATION_ENV +VIRTUAL_ENV ~PATH
```
Test if environment variables have been loaded into shell: `echo $IMMUNIZATION_ENV`. This should print `local`


10. Run `make test` to run unit tests. At this point aprox 11 test would fail out of 241 tests. (To investigate)


## Devtools - Localstack

### About 
LocalStack is a fully functional local cloud service emulator that allows developers to run AWS services locally without connecting to the actual AWS cloud. It is especially useful for testing, development, and CI/CD pipelines where real AWS resources are not needed or too costly.

### Setup:
1. Install aws cli & awslocal (for localstack) for local testing of the infrastructure, might need to install unzip. AWSLocal is a wrapper for aws that simplifies interaction with LocalStack.

```
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

pip install awscli-local
```
2. Install terraform by following the instructions from `https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli`

3. Navigate to `devtools`. 
4. Create a virtual environment in devtools: `python -m venv .venv`.
5. Activate virtual environment: `source .venv/bin/activate`. You should see a `(.venv)` as a prefix in your terminal.
6. Upgrade pip in the new environment: `pip install --upgrade pip`.
7. Run `pip install -r requirements.txt` to install packages.
8. Run `make localstack` to setup localstack to run in docker.
9. Run `make init` or `terraform init`. If you get an error about failing to install providers, then remove the `.terraform.lock.hcl` and try again.
10. Run `make seed` to create a dynamodb table in localstack and add some data into it.
11. Run the following command to get a list of 10 items from dynamodb from localstack: 
    ```
    awslocal dynamodb query \
        --table-name imms-default-imms-events \
        --key-condition-expression "PK = :pk" \
        --expression-attribute-values '{":pk": {"S": "Immunization#e3e70682-c209-4cac-629f-6fbed82c07cd"}}'.
    ```
12. If you want to delete the table run `terraform apply -destroy`

## Interacting with localstack

The idea with localstack in regards to our project is to have a dynamodb table or an s3 bucket to interact with. We can't setup all the infrastructure on localstack because of the high complexity and lack of certain features such as AWS networking and IAM enforcement.

1. Check if localstack is running in docker by calling: `docker ps`. The output should display a running container with the image localstack/localstack
2. Ensure that you have a dynamodb table set up and some data inside the table provided by the `make seed` command.
3. You can install dynamodb-admin which is npm web app that connects to localstack, to set it up: 
```
npm install -g dynamodb-admin

export DYNAMO_ENDPOINT=http://localhost:4566
export AWS_REGION=us-east-1

dynamodb-admin

# Navigate to the url provided, typically: http://localhost:8001/ where you should see the table imms-default-imms-events
```

4. To interact with the code there are 2 options. You can either persist the application lambda via docker or you can directly run / debug the code directly in vscode. Please note some modifications are needed to configure the code to run successfuly. 

4.1 To run it via vs code, we can try the get_imms_handler, but first we should ensure that the request is correct so ensure that `event` has the folowing details. Note: that we are trying to retrieve the following immunisation record form the sample data `e3e70682-c209-4cac-629f-6fbed82c07cd` hence why we hardcoded the `VaccineTypePermissions`.

```
event = {
        "pathParameters": {"id": args.id},
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "AuthenticationType": "ApplicationRestricted",
            "Permissions": (",".join([Permission.READ])),
            "VaccineTypePermissions": "covidcode2:read"
        },
    }
```
4.2 If you want to run it via docker make the following changes in the lambda.Dockerfile:
- Set the dynamo db table name env variable to `imms-default-imms-events` 
- Add `ENV DYNAMODB_TABLE_NAME=imms-default-imms-events` into the base section of the file
- Add the following line at the end in the lambda.Dockerfile:  `CMD ["get_status_handler.get_status_handler"]`
- Test by sending a request via Postman to `http://localhost:8080/2015-03-31/functions/function/invocations` and add the event data into the body section.

