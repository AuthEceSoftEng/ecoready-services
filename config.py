from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

# Load environment variables from a .env file if it exists
load_dotenv()

class Settings(BaseSettings):
    app_name: str = os.getenv("APP_NAME", "services")
    environment: str = os.getenv("ENVIRONMENT", "development")  # dev/prod/testing
    admin_username: str = os.getenv("ADMIN_USERNAME", "")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "")
    # JWT Settings
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "supersecretkey")  # Default secret key (replace in prod)
    jwt_algorithm: str = os.getenv("JWT_ALGORITHM", "HS256")  # Default algorithm for JWT
    jwt_expiration_minutes: int = int(os.getenv("JWT_EXPIRATION_MINUTES", 230))  # Token expiration time

    # Kafka Settings
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "localhost:59498")

    # Cassandra Settings
    cassandra_contact_points: str = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost")
    cassandra_port: int = int(os.getenv("CASSANDRA_PORT", 9042))
    cassandra_keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "metadata")

    # Other Settings
    api_key_secret: str = os.getenv("API_KEY_SECRET", "default-api-key-secret")

    class Config:
        case_sensitive = True  # Ensure environment variables are case sensitive


# Instantiate and use the settings
settings = Settings()