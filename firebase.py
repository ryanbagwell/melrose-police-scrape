import pyrebase
import os

firebase_config = {
    "apiKey": os.environ.get('FIREBASE_API_KEY'),
    "authDomain": os.environ.get('FIREBASE_DOMAIN'),
    "databaseURL": os.environ.get('FIREBASE_DB_URL'),
    "storageBucket": None,
    "serviceAccount": os.path.join(os.path.dirname(__file__), 'firebase-credentials.json'),
}

firebase = pyrebase.initialize_app(firebase_config)
