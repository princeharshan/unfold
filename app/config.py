from pydantic import BaseSettings

class Settings(BaseSettings):
    redditClientID: str
    redditClientSecret: str
    redditUserAgent: str
    redditUsername: str
    redditPassword: str
    twitterAPIKey: str
    twitterAPISecret: str
    twitterBearerToken: str
    twitterAccessToken: str
    twitterAccessTokenSecret: str
    openaiKey: str
    openaiOrg: str
    pineconeKey: str
    pineconeEnv: str
    PostgreSQLPassword: str
    PostgreSQLUser: str
    PostgreSQLHost: str
    PostgreSQLDatabase: str

    class Config:
        env_file = ".env"

settings = Settings()