from scheduler.config import Settings
from scheduler.store import Store


if __name__ == "__main__":
    settings = Settings.from_env()
    Store(settings.database_url, settings.schema).initialize()
    print("Scheduler schema initialized.")

