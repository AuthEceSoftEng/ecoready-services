# ecoready-services

Ecoready Services exposes a Swagger UI, via which anyone can see the services and call them (if they are authorized), located at http://155.207.19.243:8000/docs .


## Getting Started 

These instructions will help you set up and run the FastAPI application on your local machine.

### Prerequisites

Make sure you have Python and pip installed on your machine.

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/AuthEceSoftEng/ecoready-services.git
    cd ecoready-services
    ```

2. Create a virtual environment and activate it:
    ```bash 
    python -m venv env
    source env/bin/activate  # On Windows use `env\Scripts\activate`
    ```

3. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Application

To deploy the Ecoready services, run the following command:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The page of the server should be at `http://localhost:8000/docs`
